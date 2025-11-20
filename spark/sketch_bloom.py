"""Bloom-filter helper utilities for duplicate review detection.

The duplicate key is defined as the tuple
``(user_id, business_id, normalized_text)`` where ``normalized_text`` lowers
case and collapses internal whitespace. This captures exact duplicate reviews
from the same user for the same business even if punctuation or spacing
differs slightly.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

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
    reset_interval_minutes: int = 60
    watermark_minutes: int = 2


class BloomDuplicateTracker:
    """Holds a driver-side bloom filter and persists duplicate metrics."""

    def __init__(self, spark_session, config: BloomConfig | None = None) -> None:
        self.spark = spark_session
        self.config = config or BloomConfig()
        self._filter = self._build_filter()
        self._filter_created_at = datetime.utcnow()
        self._items_in_filter = 0
        self._window_stats: dict[datetime, dict[str, int]] = defaultdict(
            lambda: {"total": 0, "dup_count": 0}
        )
        self._latest_window_time: datetime | None = None

    def _build_filter(self) -> BloomFilter:
        return BloomFilter(
            capacity=self.config.capacity,
            error_rate=self.config.error_rate,
        )

    def _maybe_reset_filter(self) -> None:
        """Reset the Bloom filter periodically to avoid unbounded growth."""
        should_reset = False
        if self._items_in_filter >= self.config.capacity * 0.8:
            should_reset = True
        elif datetime.utcnow() - self._filter_created_at >= timedelta(
            minutes=self.config.reset_interval_minutes
        ):
            should_reset = True

        if should_reset:
            self._filter = self._build_filter()
            self._filter_created_at = datetime.utcnow()
            self._items_in_filter = 0
            print(
                "[Bloom] Reset in-memory filter "
                f"(interval={self.config.reset_interval_minutes}m)"
            )

    def process_batch(self, batch_df, batch_id: int) -> None:  # pragma: no cover (spark hook)
        cols = ["event_time", "review_id", "user_id", "business_id", "stars", "text"]
        pdf = batch_df.select(*cols).toPandas()
        batch_size = len(pdf)
        if batch_size == 0:
            self._flush_finalized_windows()
            return

        self._maybe_reset_filter()

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

        if not metrics.empty:
            for _, row in metrics.iterrows():
                window_start = row["window_start"].to_pydatetime()
                stats = self._window_stats[window_start]
                stats["total"] += int(row["total"])
                stats["dup_count"] += int(row["dup_count"])
                if (
                    self._latest_window_time is None
                    or window_start > self._latest_window_time
                ):
                    self._latest_window_time = window_start

        self._flush_finalized_windows()

    def _check_and_update(self, key: str) -> bool:
        seen = key in self._filter
        self._filter.add(key)
        self._items_in_filter += 1
        return seen

    def _flush_finalized_windows(self) -> None:
        """Write aggregated metrics for windows that passed the watermark."""
        if self._latest_window_time is None or not self._window_stats:
            return

        cutoff = self._latest_window_time - timedelta(minutes=self.config.watermark_minutes)
        ready_windows = [w for w in self._window_stats.keys() if w <= cutoff]

        if not ready_windows:
            return

        rows = []
        for window_start in sorted(ready_windows):
            stats = self._window_stats.pop(window_start)
            total = stats["total"]
            dup_count = stats["dup_count"]
            dup_rate = (dup_count / total) if total > 0 else 0.0
            rows.append(
                {
                    "window_start": window_start,
                    "total": total,
                    "dup_count": dup_count,
                    "dup_rate": dup_rate,
                }
            )

        if rows:
            metrics_df = pd.DataFrame(rows)
            self.spark.createDataFrame(metrics_df).write.mode("append").parquet(
                self.config.metrics_sink
            )

