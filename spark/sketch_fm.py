"""Flajolet-Martin sketch for estimating distinct user counts per window.

The Flajolet-Martin algorithm estimates the number of distinct elements in a stream
using hash functions and bit patterns. For small windows, we also compute exact counts
to measure estimation error.
"""
from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd


@dataclass
class FMConfig:
    """Configuration for Flajolet-Martin distinct count estimation."""
    num_hash_functions: int = 5  # Number of independent hash functions (better accuracy)
    sink: str = "delta/fm_distinct_users"
    watermark_minutes: int = 2
    compute_exact_for_windows_under: int = 1000  # Compute exact count if window has < N users


class FlajoletMartinTracker:
    """Tracks distinct user counts per window using Flajolet-Martin sketches."""

    def __init__(self, spark_session, config: FMConfig | None = None) -> None:
        self.spark = spark_session
        self.config = config or FMConfig()
        # Map window_start -> list of max_trailing_zeros per hash function
        self._sketches: dict[datetime, list[int]] = defaultdict(
            lambda: [0] * self.config.num_hash_functions
        )
        # For small windows, also track exact distinct users for comparison
        self._exact_counts: dict[datetime, set[str]] = defaultdict(set)
        self._latest_event_time: datetime | None = None

    def process_batch(self, batch_df, batch_id: int) -> None:  # pragma: no cover (spark hook)
        """Process a micro-batch: update FM sketches and finalize old windows."""
        cols = ["event_time", "user_id"]
        pdf = batch_df.select(*cols).toPandas()
        
        if pdf.empty:
            return

        pdf["event_time"] = pd.to_datetime(pdf["event_time"])
        pdf["window_start"] = pdf["event_time"].dt.floor("min")

        # Track latest event time for watermark calculation
        batch_max_time = pdf["event_time"].max().to_pydatetime()
        if self._latest_event_time is None or batch_max_time > self._latest_event_time:
            self._latest_event_time = batch_max_time

        # Update sketches for each window in this batch
        windows_updated = []
        for window_start, group in pdf.groupby("window_start"):
            window_dt = window_start.to_pydatetime()
            user_ids = group["user_id"].dropna().unique().tolist()
            self._update_sketch(window_dt, user_ids)
            windows_updated.append(window_dt)

        print(
            f"[FM] batch {batch_id}: rows={len(pdf)}, "
            f"windows_updated={len(windows_updated)}, "
            f"active_windows={len(self._sketches)}"
        )

        # Finalize windows that are older than watermark
        if self._latest_event_time:
            watermark_cutoff = self._latest_event_time - timedelta(minutes=self.config.watermark_minutes)
            finalized_windows = [
                w for w in list(self._sketches.keys())
                if w < watermark_cutoff
            ]

            for window_start in finalized_windows:
                self._finalize_window(window_start)

    def _update_sketch(self, window_start: datetime, user_ids: list[str]) -> None:
        """Update the FM sketch for a window with new user IDs."""
        sketch = self._sketches[window_start]
        
        # Get exact set if we're tracking it (stop tracking if window gets too large)
        exact_set = self._exact_counts.get(window_start)

        for user_id in user_ids:
            # Update exact set if we're still tracking it
            if exact_set is not None:
                exact_set.add(user_id)
                # Stop tracking exact if window gets too large (memory optimization)
                if len(exact_set) >= self.config.compute_exact_for_windows_under:
                    del self._exact_counts[window_start]
                    exact_set = None
            elif window_start not in self._exact_counts:
                # Start tracking exact for new small windows
                exact_set = set()
                self._exact_counts[window_start] = exact_set
                exact_set.add(user_id)

            # Update FM sketch: hash user_id with multiple hash functions
            for i in range(self.config.num_hash_functions):
                # Create hash with different seeds for each hash function
                hash_input = f"{user_id}_{i}".encode()
                # Use MD5 and take first 32 bits for FM algorithm
                hash_hex = hashlib.md5(hash_input).hexdigest()[:8]  # 8 hex chars = 32 bits
                hash_value = int(hash_hex, 16)
                
                # Count trailing zeros in binary representation
                trailing_zeros = self._count_trailing_zeros(hash_value)
                
                # Keep the maximum trailing zeros seen for this hash function
                if trailing_zeros > sketch[i]:
                    sketch[i] = trailing_zeros

    def _count_trailing_zeros(self, n: int) -> int:
        """Count trailing zeros in binary representation of n (32-bit hash)."""
        if n == 0:
            return 32  # Max for 32-bit hash
        count = 0
        while n & 1 == 0 and count < 32:
            count += 1
            n >>= 1
        return count

    def _estimate_distinct(self, sketch: list[int]) -> float:
        """Estimate distinct count from FM sketch using harmonic mean."""
        # Flajolet-Martin estimate: 2^R where R is the average of max trailing zeros
        # Using harmonic mean of multiple hash functions for better accuracy
        if not sketch or all(r == 0 for r in sketch):
            return 0.0
        
        # Average of 2^R for each hash function
        estimates = [2.0 ** r for r in sketch if r > 0]
        if not estimates:
            return 0.0
        
        # Use harmonic mean for better accuracy with multiple hash functions
        if len(estimates) == 1:
            return estimates[0]
        
        # Harmonic mean: n / sum(1/x_i)
        harmonic_mean = len(estimates) / sum(1.0 / e for e in estimates)
        return harmonic_mean

    def _finalize_window(self, window_start: datetime) -> None:
        """Write the finalized FM estimate and exact count (if computed) to Delta."""
        sketch = self._sketches.pop(window_start, [0] * self.config.num_hash_functions)
        exact_set = self._exact_counts.pop(window_start, None)

        if not sketch or all(r == 0 for r in sketch):
            return

        # Compute estimate
        estimate = self._estimate_distinct(sketch)
        
        # Get exact count if we tracked it
        exact_count = len(exact_set) if exact_set is not None else None

        # Calculate error if we have exact count
        error_pct = None
        if exact_count is not None and exact_count > 0:
            error_pct = abs(estimate - exact_count) / exact_count

        # Create result row
        result = pd.DataFrame([{
            "window_start": window_start,
            "distinct_users_estimate": estimate,
            "distinct_users_exact": exact_count,
            "error_percent": error_pct,
            "max_trailing_zeros": max(sketch),
            "num_hash_functions": self.config.num_hash_functions,
        }])

        # Write to Delta
        self.spark.createDataFrame(result).write.mode("append").parquet(
            self.config.sink
        )

        exact_str = f", exact={exact_count}, error={error_pct:.2%}" if exact_count is not None else ""
        print(
            f"[FM] Finalized window {window_start}: "
            f"estimate={estimate:.1f}{exact_str}"
        )

