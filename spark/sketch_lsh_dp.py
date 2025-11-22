"""MinHash LSH + Differential Privacy pipeline for Yelp-style reviews.

This job reads reviews from the bronze Delta/Parquet table, computes MinHash
signatures over review text, uses an LSH index to detect similar reviews, and
writes both raw pairs and a differentially private summary to Delta outputs.
"""
from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Optional

import numpy as np
import pandas as pd
from datasketch import MinHash, MinHashLSH
from pyspark.sql import DataFrame, SparkSession, functions as F

# Ensure Spark driver and workers use the same Python executable (e.g., python3.12)
current_python = sys.executable
os.environ.setdefault("PYSPARK_PYTHON", current_python)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", current_python)

TOKEN_PATTERN = re.compile(r"[a-z0-9]+")


def _get_env_int(name: str, default: Optional[int]) -> Optional[int]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _get_env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _get_env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_env_keywords(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return [part.strip().lower() for part in value.split(",") if part.strip()]


@dataclass
class LSHConfig:
    """Runtime configuration for the MinHash LSH DP job."""

    source_path: str = "delta/bronze_reviews"
    source_format: str = "parquet"
    text_column: str = "text"
    event_time_column: str = "event_time"
    raw_sink: str = "delta/dup_lsh_raw"
    dp_sink: str = "delta/dup_lsh_dp"
    num_perm: int = 128
    lsh_threshold: float = 0.85
    min_jaccard_score: float = 0.85
    min_token_length: int = 3
    shingle_size: int = 1  # 1 => word-level MinHash
    max_reviews: Optional[int] = None
    max_candidates_per_review: Optional[int] = None
    dp_epsilon: float = 1.0
    dp_sensitivity: float = 1.0
    dp_min_noisy_count: float = 0.0
    random_seed: int = 42
    filter_bug_reviews: bool = True
    bug_keywords: list[str] = field(
        default_factory=lambda: [
            "bug",
            "bugs",
            "buggy",
            "crash",
            "crashing",
            "lag",
            "laggy",
            "slow",
            "glitch",
            "error",
            "issue",
            "problem",
            "broken",
            "freeze",
            "frozen",
            "hang",
            "fail",
            "failure",
        ]
    )
    bug_max_star: Optional[float] = 3.0
    bug_summary_sink: str = "delta/dup_lsh_bug_summary"

    @property
    def dp_laplace_scale(self) -> float:
        if self.dp_epsilon <= 0:
            raise ValueError("dp_epsilon must be > 0 for Laplace mechanism")
        return self.dp_sensitivity / self.dp_epsilon

    @property
    def bug_keyword_pattern(self) -> str:
        if not self.bug_keywords:
            return ""
        escaped = [re.escape(k) for k in self.bug_keywords]
        return "|".join(escaped)

    @classmethod
    def from_env(cls) -> "LSHConfig":
        default_keywords = [
            "bug",
            "bugs",
            "buggy",
            "crash",
            "crashing",
            "lag",
            "laggy",
            "slow",
            "glitch",
            "error",
            "issue",
            "problem",
            "broken",
            "freeze",
            "frozen",
            "hang",
            "fail",
            "failure",
        ]
        return cls(
            source_path=_get_env_str("LSH_SOURCE_PATH", "delta/bronze_reviews"),
            source_format=_get_env_str("LSH_SOURCE_FORMAT", "parquet"),
            text_column=_get_env_str("LSH_TEXT_COLUMN", "text"),
            event_time_column=_get_env_str("LSH_EVENT_TIME_COLUMN", "event_time"),
            raw_sink=_get_env_str("LSH_RAW_SINK", "delta/dup_lsh_raw"),
            dp_sink=_get_env_str("LSH_DP_SINK", "delta/dup_lsh_dp"),
            num_perm=_get_env_int("LSH_NUM_PERM", 128) or 128,
            lsh_threshold=_get_env_float("LSH_THRESHOLD", 0.85),
            min_jaccard_score=_get_env_float("LSH_MIN_JACCARD", 0.85),
            min_token_length=_get_env_int("LSH_MIN_TOKEN_LENGTH", 3) or 3,
            shingle_size=_get_env_int("LSH_SHINGLE_SIZE", 1) or 1,
            max_reviews=_get_env_int("LSH_MAX_REVIEWS", None),
            max_candidates_per_review=_get_env_int("LSH_MAX_CANDIDATES", None),
            dp_epsilon=_get_env_float("LSH_DP_EPSILON", 1.0),
            dp_sensitivity=_get_env_float("LSH_DP_SENSITIVITY", 1.0),
            dp_min_noisy_count=_get_env_float("LSH_DP_MIN_NOISY_COUNT", 0.0),
            random_seed=_get_env_int("LSH_RANDOM_SEED", 42) or 42,
            filter_bug_reviews=_get_env_bool("LSH_FILTER_BUG_REVIEWS", True),
            bug_keywords=_get_env_keywords("LSH_BUG_KEYWORDS", default_keywords),
            bug_max_star=_get_env_float("LSH_BUG_MAX_STAR", 3.0),
            bug_summary_sink=_get_env_str("LSH_BUG_SUMMARY_SINK", "delta/dup_lsh_bug_summary"),
        )


def tokenize(text: str, min_token_length: int) -> list[str]:
    """Lowercase, keep alphanumeric tokens, drop very short tokens."""
    if not text:
        return []
    tokens = TOKEN_PATTERN.findall(text.lower())
    return [tok for tok in tokens if len(tok) >= min_token_length]


def build_minhash(tokens: Iterable[str], num_perm: int) -> Optional[MinHash]:
    """Build a MinHash signature from iterable tokens."""
    tokens = list(tokens)
    if not tokens:
        return None
    mh = MinHash(num_perm=num_perm)
    for token in tokens:
        mh.update(token.encode("utf-8"))
    return mh


def extract_keyword_matches(text: str, keyword_regex: Optional[re.Pattern]) -> list[str]:
    """Return sorted unique keyword matches from text."""
    if not text or not keyword_regex:
        return []
    matches = {m.group(0).lower() for m in keyword_regex.finditer(text)}
    return sorted(matches)


class MinHashLSHDPJob:
    """Encapsulates MinHash LSH pair detection and DP aggregation."""

    def __init__(self, spark: SparkSession, config: LSHConfig) -> None:
        self.spark = spark
        self.config = config
        self._rng = np.random.default_rng(config.random_seed)
        self._keyword_regex = (
            re.compile(self.config.bug_keyword_pattern, re.IGNORECASE)
            if self.config.bug_keyword_pattern
            else None
        )

    def run(self) -> None:
        reviews_df = self._load_reviews()
        if reviews_df is None or reviews_df.rdd.isEmpty():
            print("[LSH] No reviews found at source, exiting")
            return

        raw_pairs_df = self._compute_similar_pairs(reviews_df)
        if raw_pairs_df is None or raw_pairs_df.rdd.isEmpty():
            print("[LSH] No similar-review pairs detected; nothing to write")
            return

        self._write_output(raw_pairs_df, self.config.raw_sink, label="raw")

        dp_pairs_df = self._apply_differential_privacy(raw_pairs_df)
        if dp_pairs_df is None or dp_pairs_df.rdd.isEmpty():
            print("[LSH] DP filtering removed all pairs; skipping DP sink write")
            return

        self._write_output(dp_pairs_df, self.config.dp_sink, label="dp")

        bug_summary_df = self._summarize_bug_clusters(dp_pairs_df)
        if bug_summary_df is not None and not bug_summary_df.rdd.isEmpty():
            self._write_output(
                bug_summary_df, self.config.bug_summary_sink, label="bug_summary"
            )

    def _load_reviews(self) -> DataFrame:
        """Load review data from configured source and select needed columns."""
        df = (
            self.spark.read.format(self.config.source_format)
            .load(self.config.source_path)
        )

        required_columns = {
            "review_id",
            "business_id",
            self.config.text_column,
            "stars",
        }
        missing = required_columns - set(df.columns)
        if missing:
            raise ValueError(f"Source data missing columns: {missing}")

        df, event_col = self._resolve_event_column(df)
        selected = (
            df.select(
                F.col("review_id"),
                F.col("business_id"),
                F.col(self.config.text_column).alias("text"),
                F.col(event_col).alias("event_time"),
                F.col("stars"),
            )
            .dropna(subset=["review_id", "business_id", "text"])
            # Ensure each review_id appears only once for the LSH index
            .dropDuplicates(["review_id"])
        )

        if self.config.filter_bug_reviews and self.config.bug_keyword_pattern:
            selected = selected.filter(
                F.lower(F.col("text")).rlike(self.config.bug_keyword_pattern)
            )
            if self.config.bug_max_star is not None:
                selected = selected.filter(F.col("stars") <= self.config.bug_max_star)

        if self.config.max_reviews:
            selected = (
                selected.orderBy(F.col("event_time").asc_nulls_last())
                .limit(self.config.max_reviews)
            )

        return selected

    def _resolve_event_column(self, df: DataFrame) -> tuple[DataFrame, str]:
        """Choose an event-time column with sensible fallbacks."""
        candidates = [
            self.config.event_time_column,
            "ingest_time",
            "event_time",
            "date",
        ]
        for candidate in candidates:
            if candidate in df.columns:
                return df, candidate
        # fallback: synthesize current timestamp for lack of better signal
        df_with_ts = df.withColumn("_synthetic_event_time", F.current_timestamp())
        return df_with_ts, "_synthetic_event_time"

    def _compute_similar_pairs(self, reviews_df: DataFrame) -> Optional[DataFrame]:
        """Build MinHash signatures, query LSH index, and emit raw pairs."""
        pdf = reviews_df.toPandas()
        if pdf.empty:
            return None

        pdf = pdf.drop_duplicates(subset=["review_id"])
        if pdf.empty:
            return None

        print(f"[LSH] Building LSH over {len(pdf)} reviews")
        lsh = MinHashLSH(
            threshold=self.config.lsh_threshold, num_perm=self.config.num_perm
        )
        signatures: dict[str, MinHash] = {}
        metadata: dict[str, dict[str, object]] = {}
        batch_time = datetime.now(timezone.utc)
        batch_id = batch_time.strftime("%Y%m%d%H%M%S")
        processing_time = batch_time
        records: list[dict] = []

        for row in pdf.itertuples(index=False):
            tokens = tokenize(getattr(row, "text", ""), self.config.min_token_length)
            if not tokens:
                continue

            mh = build_minhash(tokens, self.config.num_perm)
            if mh is None:
                continue

            review_keywords = extract_keyword_matches(
                getattr(row, "text", ""), self._keyword_regex
            )
            review_keywords_str = ",".join(review_keywords)

            candidates = lsh.query(mh)
            if self.config.max_candidates_per_review:
                candidates = candidates[: self.config.max_candidates_per_review]

            for candidate_id in candidates:
                if candidate_id == row.review_id or candidate_id not in signatures:
                    continue
                candidate_mh = signatures[candidate_id]
                score = mh.jaccard(candidate_mh)
                if score < self.config.min_jaccard_score:
                    continue
                candidate_meta = metadata[candidate_id]
                records.append(
                    {
                        "batch_id": batch_id,
                        "processing_time": processing_time,
                        "review_id": row.review_id,
                        "business_id": row.business_id,
                        "review_event_time": row.event_time,
                        "similar_review_id": candidate_id,
                        "similar_business_id": candidate_meta["business_id"],
                        "similar_event_time": candidate_meta["event_time"],
                        "approx_jaccard": float(score),
                        "review_stars": row.stars,
                        "similar_review_stars": candidate_meta["stars"],
                        "review_keywords": review_keywords_str,
                        "similar_keywords": candidate_meta.get("keywords", ""),
                    }
                )

            lsh.insert(row.review_id, mh)
            signatures[row.review_id] = mh
            metadata[row.review_id] = {
                "business_id": row.business_id,
                "event_time": row.event_time,
                "stars": row.stars,
                "keywords": review_keywords_str,
            }

        if not records:
            return None

        raw_pdf = pd.DataFrame(records)
        return self.spark.createDataFrame(raw_pdf)

    def _apply_differential_privacy(self, raw_pairs_df: DataFrame) -> Optional[DataFrame]:
        """Aggregate 'similar_to' counts and add Laplace noise."""
        pdf = raw_pairs_df.toPandas()
        if pdf.empty:
            return None

        counts = (
            pdf.groupby(["similar_review_id", "similar_business_id"])
            .size()
            .reset_index(name="exact_frequency")
        )

        noise = self._rng.laplace(
            loc=0.0,
            scale=self.config.dp_laplace_scale,
            size=len(counts),
        )
        counts["noisy_frequency"] = counts["exact_frequency"] + noise

        if self.config.dp_min_noisy_count is not None:
            counts = counts[counts["noisy_frequency"] >= self.config.dp_min_noisy_count]

        if counts.empty:
            return None

        dp_pdf = pdf.merge(
            counts[
                ["similar_review_id", "similar_business_id", "noisy_frequency"]
            ],
            on=["similar_review_id", "similar_business_id"],
            how="inner",
        )

        return self.spark.createDataFrame(dp_pdf)

    def _summarize_bug_clusters(self, dp_pairs_df: DataFrame) -> Optional[DataFrame]:
        if dp_pairs_df is None:
            return None
        pdf = dp_pairs_df.toPandas()
        if pdf.empty:
            return None

        pdf["star_gap"] = (pdf["review_stars"] - pdf["similar_review_stars"]).abs()
        if self.config.bug_max_star is not None:
            threshold = self.config.bug_max_star
            pdf["low_star_pair"] = (
                (pdf["review_stars"] <= threshold)
                | (pdf["similar_review_stars"] <= threshold)
            ).astype(float)
        else:
            pdf["low_star_pair"] = 0.0

        def _keyword_span(series: pd.Series) -> int:
            keywords: set[str] = set()
            for entry in series:
                if not entry:
                    continue
                keywords.update(k for k in entry.split(",") if k)
            return len(keywords)

        summary = (
            pdf.groupby("business_id")
            .agg(
                bug_pair_count=("review_id", "count"),
                avg_review_stars=("review_stars", "mean"),
                avg_similar_stars=("similar_review_stars", "mean"),
                avg_jaccard=("approx_jaccard", "mean"),
                avg_star_gap=("star_gap", "mean"),
                max_noisy_frequency=("noisy_frequency", "max"),
                low_star_pair_rate=("low_star_pair", "mean"),
            )
            .reset_index()
        )

        keyword_counts = (
            pdf.groupby("business_id")["review_keywords"].apply(_keyword_span).reset_index()
        )
        keyword_counts = keyword_counts.rename(columns={"review_keywords": "keyword_coverage"})
        summary = summary.merge(keyword_counts, on="business_id", how="left")
        return self.spark.createDataFrame(summary)

    def _write_output(self, df: DataFrame, path: str, label: str) -> None:
        """Write DataFrame to Delta/Parquet sink with append semantics."""
        row_count = df.count()
        if row_count == 0:
            print(f"[LSH] {label} DataFrame empty; skipping write to {path}")
            return
        (
            df.write.mode("append")
            .format("parquet")
            .save(path)
        )
        print(f"[LSH] Wrote {row_count} {label} rows to {path}")


def main() -> None:
    spark = (
        SparkSession.builder.appName("reviews-minhash-lsh-dp")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    config = LSHConfig.from_env()
    job = MinHashLSHDPJob(spark, config)
    job.run()
    spark.stop()


if __name__ == "__main__":
    main()

