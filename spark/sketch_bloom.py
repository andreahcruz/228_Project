"""Bloom-filter helper utilities for duplicate review detection.

The duplicate key is defined as the tuple
``(user_id, business_id, normalized_text)`` where ``normalized_text`` lowers
case and collapses internal whitespace. This captures exact duplicate reviews
from the same user for the same business even if punctuation or spacing
differs slightly.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from pybloom_live import BloomFilter


def _normalize_text(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def build_duplicate_key(series: pd.Series, other: pd.Series, text: pd.Series) -> pd.Series:
    """Build the deduplication key using normalized text."""
    return (
        series.fillna("")
        + "||"
        + other.fillna("")
        + "||"
        + _normalize_text(text)
    )


@dataclass
class BloomConfig:
    capacity: int = 200_000
    error_rate: float = 0.01
    dup_sink: str = "delta/dup_bloom"
    metrics_sink: str = "delta/gold_dup_rate_1m"


class BloomDuplicateTracker:
    """Holds a driver-side bloom filter and persists duplicate metrics."""

    def __init__(self, spark_session, config: BloomConfig | None = None) -> None:
        self.spark = spark_session
        self.config = config or BloomConfig()
        self._filter = BloomFilter(
            capacity=self.config.capacity,
            error_rate=self.config.error_rate,
        )

    def process_batch(self, batch_df, batch_id: int) -> None:  # pragma: no cover (spark hook)
        cols = ["event_time", "review_id", "user_id", "business_id", "stars", "text"]
        pdf = batch_df.select(*cols).toPandas()
        batch_size = len(pdf)
        if batch_size == 0:
            return

        pdf["dup_key"] = build_duplicate_key(pdf["user_id"], pdf["business_id"], pdf["text"])
        pdf["is_duplicate"] = pdf["dup_key"].apply(self._check_and_update)
        pdf["window_start"] = pdf["event_time"].dt.floor("min")

        dup_rows = pdf.loc[pdf["is_duplicate"]]
        if not dup_rows.empty:
            self.spark.createDataFrame(dup_rows[cols + ["dup_key"]]).write.mode("append").parquet(
                self.config.dup_sink
            )

        metrics = (
            pdf.groupby("window_start")
            .agg(total=("review_id", "size"), dup_count=("is_duplicate", "sum"))
            .reset_index()
        )
        metrics["dup_rate"] = metrics["dup_count"] / metrics["total"]
        self.spark.createDataFrame(metrics).write.mode("append").parquet(self.config.metrics_sink)

    def _check_and_update(self, key: str) -> bool:
        seen = key in self._filter
        self._filter.add(key)
        return seen

