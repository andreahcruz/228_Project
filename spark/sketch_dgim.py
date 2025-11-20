"""DGIM (Datar-Gionis-Indyk-Motwani) algorithm for counting 1s in a sliding window.

The DGIM algorithm estimates the number of 1s in the last W minutes using sublinear memory.
Each review is mapped to a bit: 1 if stars >= 4, 0 otherwise.
Maintains buckets per minute over a logical sliding window.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd


@dataclass
class DGIMConfig:
    """Configuration for DGIM sliding window count estimation."""
    window_minutes: int = 60  # Size of sliding window in minutes
    sink: str = "delta/gold_dgim_highstar"
    compute_exact: bool = True  # Also compute exact count for comparison (optional)


class DGIMTracker:
    """Tracks count of high-star reviews (stars >= 4) in a sliding window using DGIM."""

    def __init__(self, spark_session, config: DGIMConfig | None = None) -> None:
        self.spark = spark_session
        self.config = config or DGIMConfig()
        
        # DGIM buckets: list of (timestamp, size) tuples
        # Each bucket represents a contiguous segment of 1s
        # Buckets are ordered by timestamp (oldest first)
        self._buckets: deque[tuple[datetime, int]] = deque()
        
        # For exact count (optional): track all 1s in the window
        self._exact_bits: deque[tuple[datetime, bool]] = deque()  # (timestamp, bit_value)
        
        # Track latest timestamp seen
        self._latest_timestamp: datetime | None = None

    def _add_bit(self, timestamp: datetime, bit: bool) -> None:
        """Add a bit to the DGIM structure."""
        if not bit:
            # 0s don't need to be stored, just update timestamp
            if self._latest_timestamp is None or timestamp > self._latest_timestamp:
                self._latest_timestamp = timestamp
            return
        
        # Add 1 to the structure
        self._buckets.append((timestamp, 1))
        
        # Maintain DGIM invariant: at most 2 buckets of each size
        self._merge_buckets()
        
        # Update latest timestamp
        if self._latest_timestamp is None or timestamp > self._latest_timestamp:
            self._latest_timestamp = timestamp
        
        # For exact count (optional)
        if self.config.compute_exact:
            self._exact_bits.append((timestamp, True))
            self._clean_exact_bits(timestamp)

    def _merge_buckets(self) -> None:
        """Merge buckets to maintain DGIM invariant: at most 2 buckets of each size."""
        # Convert deque to list for easier manipulation
        buckets_list = list(self._buckets)
        if len(buckets_list) < 3:
            return
        
        # Group buckets by size
        size_groups: dict[int, list[tuple[datetime, int, int]]] = {}
        for idx, (timestamp, size) in enumerate(buckets_list):
            if size not in size_groups:
                size_groups[size] = []
            size_groups[size].append((timestamp, size, idx))
        
        # Check if any size has 3+ buckets
        merged = False
        for size, bucket_list in size_groups.items():
            if len(bucket_list) >= 3:
                # Sort by timestamp (oldest first)
                bucket_list.sort(key=lambda x: x[0])
                # Merge the two oldest buckets of this size
                oldest1 = bucket_list[0]
                oldest2 = bucket_list[1]
                
                # Create new merged bucket
                merged_size = size * 2
                merged_timestamp = min(oldest1[0], oldest2[0])
                
                # Rebuild buckets list: remove the two oldest, add merged
                new_buckets = []
                for idx, (ts, sz) in enumerate(buckets_list):
                    if idx not in (oldest1[2], oldest2[2]):
                        new_buckets.append((ts, sz))
                new_buckets.append((merged_timestamp, merged_size))
                
                # Sort by timestamp to maintain order
                new_buckets.sort(key=lambda x: x[0])
                self._buckets = deque(new_buckets)
                
                merged = True
                break
        
        # Recursively merge if we merged something
        if merged:
            self._merge_buckets()

    def _clean_buckets(self, current_time: datetime) -> None:
        """Remove buckets that are completely outside the sliding window."""
        window_start = current_time - timedelta(minutes=self.config.window_minutes)
        
        # Remove buckets that are completely before the window
        while self._buckets and self._buckets[0][0] < window_start:
            # Check if bucket is completely outside window
            # A bucket of size s at time t covers [t, t+s minutes)
            bucket_time, bucket_size = self._buckets[0]
            bucket_end = bucket_time + timedelta(minutes=bucket_size)
            
            if bucket_end <= window_start:
                self._buckets.popleft()
            else:
                # Bucket is partially in window, keep it
                break

    def _clean_exact_bits(self, current_time: datetime) -> None:
        """Remove bits outside the sliding window for exact count."""
        if not self.config.compute_exact:
            return
        
        window_start = current_time - timedelta(minutes=self.config.window_minutes)
        
        while self._exact_bits and self._exact_bits[0][0] < window_start:
            self._exact_bits.popleft()

    def _estimate_count(self, current_time: datetime) -> int:
        """Estimate the number of 1s in the last W minutes using DGIM."""
        if not self._latest_timestamp or not self._buckets:
            return 0
        
        window_start = current_time - timedelta(minutes=self.config.window_minutes)
        
        # Clean old buckets
        self._clean_buckets(current_time)
        
        if not self._buckets:
            return 0
        
        # DGIM estimation: sum all bucket sizes that are completely within the window,
        # plus half of the oldest bucket that might be partially outside
        estimate = 0
        oldest_bucket_added = False
        
        for bucket_time, bucket_size in self._buckets:
            bucket_end = bucket_time + timedelta(minutes=bucket_size)
            
            if bucket_end <= window_start:
                # Bucket is completely outside the window, skip
                continue
            elif bucket_time >= window_start:
                # Bucket is completely within the window
                estimate += bucket_size
            else:
                # Bucket starts before window but overlaps with it
                # This is the oldest bucket that overlaps
                if not oldest_bucket_added:
                    # Add half of this bucket (DGIM estimation technique)
                    estimate += bucket_size // 2
                    oldest_bucket_added = True
        
        return max(0, estimate)

    def _exact_count(self, current_time: datetime) -> int:
        """Compute exact count of 1s in the last W minutes (optional)."""
        if not self.config.compute_exact:
            return None
        
        self._clean_exact_bits(current_time)
        return sum(1 for _, bit in self._exact_bits if bit)

    def process_batch(self, batch_df, batch_id: int) -> None:  # pragma: no cover (spark hook)
        """Process a micro-batch: map reviews to bits and update DGIM structure."""
        cols = ["event_time", "stars"]
        pdf = batch_df.select(*cols).toPandas()
        
        if pdf.empty:
            return

        pdf["event_time"] = pd.to_datetime(pdf["event_time"], utc=True)
        pdf["is_high_star"] = pdf["stars"] >= 4.0
        
        # Update DGIM structure for each review
        for _, row in pdf.iterrows():
            timestamp = row["event_time"].to_pydatetime()
            bit = bool(row["is_high_star"])
            self._add_bit(timestamp, bit)
        
        # Get current time (use latest timestamp or current time)
        current_time = self._latest_timestamp if self._latest_timestamp else datetime.now()
        
        # Estimate count in sliding window
        estimate = self._estimate_count(current_time)
        exact = self._exact_count(current_time) if self.config.compute_exact else None
        
        # Write estimate to Delta
        result_df = pd.DataFrame([{
            "ts": current_time,
            "window_minutes": self.config.window_minutes,
            "estimate": estimate,
            "exact": exact,
        }])
        
        self.spark.createDataFrame(result_df).write.mode("append").parquet(
            self.config.sink
        )
        
        exact_str = f", exact={exact}" if exact is not None else ""
        print(
            f"[DGIM] batch {batch_id}: estimate={estimate}{exact_str}, "
            f"buckets={len(self._buckets)}, window={self.config.window_minutes}min"
        )

