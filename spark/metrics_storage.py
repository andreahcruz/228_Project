"""Storage metrics tracking for checkpoint and state size growth.

Tracks the size of checkpoint directories and state storage over time.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


@dataclass
class StorageConfig:
    """Configuration for storage metrics tracking."""
    sink: str = "delta/storage_metrics"
    checkpoint_dirs: list[str] | None = None  # If None, auto-detect from delta/


def get_directory_size(path: Path) -> int:
    """Recursively calculate total size of directory in bytes."""
    total = 0
    try:
        for entry in path.rglob("*"):
            if entry.is_file():
                total += entry.stat().st_size
    except (OSError, PermissionError):
        pass  # Skip files we can't access
    return total


class StorageTracker:
    """Tracks checkpoint and state storage sizes over time."""

    def __init__(self, spark_session, config: StorageConfig | None = None) -> None:
        self.spark = spark_session
        self.config = config or StorageConfig()
        self._delta_dir = Path("delta")
        
        # Auto-detect checkpoint directories if not specified
        if self.config.checkpoint_dirs is None:
            self.config.checkpoint_dirs = [
                str(d) for d in self._delta_dir.glob("_ckpt_*")
                if d.is_dir()
            ]

    def record_metrics(self) -> None:
        """Record current storage metrics and write to Delta."""
        timestamp = datetime.now()
        metrics = []

        for checkpoint_dir in self.config.checkpoint_dirs:
            path = Path(checkpoint_dir)
            if not path.exists():
                continue

            size_bytes = get_directory_size(path)
            size_mb = size_bytes / (1024 * 1024)

            metrics.append({
                "timestamp": timestamp,
                "checkpoint_name": path.name,
                "checkpoint_path": str(path),
                "size_bytes": size_bytes,
                "size_mb": size_mb,
            })

        if not metrics:
            return

        df = pd.DataFrame(metrics)
        self.spark.createDataFrame(df).write.mode("append").parquet(
            self.config.sink
        )

        total_size_mb = df["size_mb"].sum()
        print(
            f"[Storage] Recorded metrics for {len(metrics)} checkpoints, "
            f"total size: {total_size_mb:.2f} MB"
        )

