"""Performance metrics tracking for streaming pipeline.

Tracks throughput (events/sec) and end-to-end latency (event_time to processing_time)
per micro-batch and writes aggregated metrics to Delta.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass
class PerformanceConfig:
    """Configuration for performance metrics tracking."""
    sink: str = "delta/performance_metrics"
    aggregation_window_minutes: int = 1  # Aggregate metrics per minute


class PerformanceTracker:
    """Tracks throughput and latency metrics from streaming batches."""

    def __init__(self, spark_session, config: PerformanceConfig | None = None) -> None:
        self.spark = spark_session
        self.config = config or PerformanceConfig()
        self._batch_metrics: list[dict] = []

    def process_batch(self, batch_df, batch_id: int) -> None:  # pragma: no cover (spark hook)
        """Process a micro-batch: measure throughput and latency."""
        try:
            batch_start_time = time.time()
            
            # Collect batch to measure size and latency
            pdf = batch_df.select("event_time").toPandas()
            
            if pdf.empty:
                print(f"[Performance] batch {batch_id}: empty batch, skipping")
                return

            batch_end_time = time.time()
            processing_time = batch_end_time - batch_start_time
            
            # Calculate throughput (events per second)
            num_events = len(pdf)
            throughput = num_events / processing_time if processing_time > 0 else 0.0

            # Calculate latency: event_time to processing_time
            # Ensure both are timezone-aware (UTC) for proper subtraction
            pdf["event_time"] = pd.to_datetime(pdf["event_time"], utc=True)
            pdf["processing_time"] = pd.to_datetime(batch_end_time, unit="s", utc=True)
            pdf["latency_seconds"] = (pdf["processing_time"] - pdf["event_time"]).dt.total_seconds()
            
            # Aggregate latency stats
            avg_latency = pdf["latency_seconds"].mean()
            p50_latency = pdf["latency_seconds"].median()
            p95_latency = pdf["latency_seconds"].quantile(0.95) if len(pdf) > 0 else 0.0
            p99_latency = pdf["latency_seconds"].quantile(0.99) if len(pdf) > 0 else 0.0
            max_latency = pdf["latency_seconds"].max()

            # Window for aggregation (1-minute buckets)
            window_start = pd.to_datetime(batch_end_time, unit="s", utc=True).floor("min")

            metric = {
                "batch_id": batch_id,
                "window_start": window_start,
                "processing_timestamp": pd.to_datetime(batch_end_time, unit="s", utc=True),
                "num_events": num_events,
                "processing_time_seconds": processing_time,
                "throughput_events_per_sec": throughput,
                "avg_latency_seconds": avg_latency,
                "p50_latency_seconds": p50_latency,
                "p95_latency_seconds": p95_latency,
                "p99_latency_seconds": p99_latency,
                "max_latency_seconds": max_latency,
            }
            
            self._batch_metrics.append(metric)
            
            print(f"[Performance] batch {batch_id}: events={num_events}, throughput={throughput:.1f}/s, latency={avg_latency:.2f}s, accumulated={len(self._batch_metrics)}")

            # Write more frequently - every 3 batches (~15 seconds) or when we have 6+ batches
            # This ensures data is available sooner for the dashboard
            # Also write on every 10th batch to ensure we don't miss data
            if len(self._batch_metrics) >= 3 or (batch_id > 0 and (batch_id % 3 == 0 or batch_id % 10 == 0)):
                self._write_aggregated_metrics()
        except Exception as e:
            print(f"[Performance] Error in process_batch batch {batch_id}: {e}")
            import traceback
            traceback.print_exc()
            # Don't re-raise - allow streaming to continue

    def _write_aggregated_metrics(self) -> None:
        """Aggregate batch metrics by window and write to Delta."""
        try:
            if not self._batch_metrics:
                print("[Performance] No batch metrics to write")
                return

            df = pd.DataFrame(self._batch_metrics)
            
            if df.empty:
                print("[Performance] Empty DataFrame, skipping write")
                return
            
            # Aggregate by window_start
            grouped = df.groupby("window_start")
            aggregated = pd.DataFrame({
                "window_start": grouped["window_start"].first(),
                "total_events": grouped["num_events"].sum(),
                "total_processing_time": grouped["processing_time_seconds"].sum(),
                "avg_throughput": grouped["throughput_events_per_sec"].mean(),
                "max_throughput": grouped["throughput_events_per_sec"].max(),
                "avg_latency": grouped["avg_latency_seconds"].mean(),
                "p50_latency": grouped["p50_latency_seconds"].median(),
                "p95_latency": grouped["p95_latency_seconds"].quantile(0.95),
                "p99_latency": grouped["p99_latency_seconds"].quantile(0.95),
                "max_latency": grouped["max_latency_seconds"].max(),
                "num_batches": grouped["batch_id"].count(),
            }).reset_index(drop=True)

            # Write to Delta
            self.spark.createDataFrame(aggregated).write.mode("append").parquet(
                self.config.sink
            )

            print(
                f"[Performance] Wrote {len(aggregated)} aggregated metrics windows "
                f"(from {len(self._batch_metrics)} batches)"
            )

            # Clear batch metrics
            self._batch_metrics.clear()
        except Exception as e:
            print(f"[Performance] Error writing aggregated metrics: {e}")
            import traceback
            traceback.print_exc()
            # Don't clear metrics on error - try again next time

