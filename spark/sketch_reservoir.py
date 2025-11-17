"""Reservoir sampling helper for per-window uniform random samples.

Maintains a reservoir sample of reviews per 1-minute time window, allowing
downstream analysis on a manageable subset while preserving statistical properties.
"""
from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd


@dataclass
class ReservoirConfig:
    """Configuration for per-window reservoir sampling."""
    sample_size: int = 100  # Number of reviews to keep per window
    sink: str = "delta/reservoir_samples"
    watermark_minutes: int = 2  # Windows older than this are finalized


class ReservoirSampler:
    """Maintains reservoir samples per time window and writes finalized samples to Delta."""

    def __init__(self, spark_session, config: ReservoirConfig | None = None) -> None:
        self.spark = spark_session
        self.config = config or ReservoirConfig()
        # Map window_start (datetime) -> list of (row_index, row_data)
        self._reservoirs: dict[datetime, list[tuple[int, dict]]] = defaultdict(list)
        # Track total items seen per window for reservoir algorithm
        self._window_counts: dict[datetime, int] = defaultdict(int)
        self._rng = random.Random(42)  # Fixed seed for reproducibility
        self._latest_event_time: datetime | None = None

    def process_batch(self, batch_df, batch_id: int) -> None:  # pragma: no cover (spark hook)
        """Process a micro-batch: update reservoir samples and finalize old windows."""
        cols = ["event_time", "review_id", "user_id", "business_id", "stars", "text"]
        pdf = batch_df.select(*cols).toPandas()
        
        if pdf.empty:
            return

        pdf["event_time"] = pd.to_datetime(pdf["event_time"])
        pdf["window_start"] = pdf["event_time"].dt.floor("min")

        # Track latest event time for watermark calculation
        batch_max_time = pdf["event_time"].max().to_pydatetime()
        if self._latest_event_time is None or batch_max_time > self._latest_event_time:
            self._latest_event_time = batch_max_time

        # Update reservoir samples for each window in this batch
        windows_updated = []
        for window_start, group in pdf.groupby("window_start"):
            window_dt = window_start.to_pydatetime()
            self._update_reservoir(window_dt, group[cols].to_dict("records"))
            windows_updated.append(window_dt)

        print(
            f"[Reservoir] batch {batch_id}: rows={len(pdf)}, "
            f"windows_updated={len(windows_updated)}, "
            f"active_windows={len(self._reservoirs)}"
        )

        # Finalize windows that are older than watermark (relative to latest event time)
        if self._latest_event_time:
            watermark_cutoff = self._latest_event_time - timedelta(minutes=self.config.watermark_minutes)
            finalized_windows = [
                w for w in list(self._reservoirs.keys())
                if w < watermark_cutoff
            ]

            for window_start in finalized_windows:
                self._finalize_window(window_start)

    def _update_reservoir(self, window_start: datetime, rows: list[dict]) -> None:
        """Update the reservoir sample for a given window using reservoir sampling algorithm."""
        reservoir = self._reservoirs[window_start]
        current_count = self._window_counts[window_start]

        for row in rows:
            current_count += 1
            # Reservoir sampling: if we haven't filled the sample, add it
            if len(reservoir) < self.config.sample_size:
                reservoir.append((current_count, row))
            else:
                # Randomly decide whether to replace an existing item
                j = self._rng.randint(1, current_count)
                if j <= self.config.sample_size:
                    # Replace item at position (j-1)
                    reservoir[j - 1] = (current_count, row)

        self._window_counts[window_start] = current_count

    def _finalize_window(self, window_start: datetime) -> None:
        """Write the finalized reservoir sample to Delta and remove from memory."""
        reservoir = self._reservoirs.pop(window_start, [])
        total_seen = self._window_counts.pop(window_start, 0)

        if not reservoir:
            return

        # Extract just the row data (drop the index)
        sample_rows = [row_data for _, row_data in reservoir]
        sample_df = pd.DataFrame(sample_rows)
        sample_df["window_start"] = window_start

        # Write to Delta
        self.spark.createDataFrame(sample_df).write.mode("append").parquet(
            self.config.sink
        )

        print(
            f"[Reservoir] Finalized window {window_start}: "
            f"sample_size={len(sample_rows)}, total_seen={total_seen}"
        )

