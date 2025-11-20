"""Systematic accuracy reporting for sketching algorithms.

Aggregates FM error metrics and performs Bloom filter false-positive/negative analysis
on small windows to validate sketch accuracy.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


@dataclass
class AccuracyConfig:
    """Configuration for accuracy reporting."""
    fm_accuracy_sink: str = "delta/accuracy_fm_summary"
    bloom_accuracy_sink: str = "delta/accuracy_bloom_summary"
    window_size_minutes: int = 60  # Aggregate accuracy over this window size
    # For this project we have a relatively small number of FM windows, so allow
    # reports with fewer exact-count windows to ensure the dashboard shows data.
    # In a production setting you might want this closer to 10 for more stable stats.
    fm_min_windows_for_report: int = 2  # Minimum windows needed before reporting (FM)
    bloom_min_windows_for_report: int = 3
    bloom_sample_windows: int = 50
    bloom_sample_files: int = 200


class AccuracyReporter:
    """Aggregates and reports accuracy metrics for FM and Bloom sketches."""

    def __init__(self, spark_session, config: AccuracyConfig | None = None) -> None:
        self.spark = spark_session
        self.config = config or AccuracyConfig()
        self._delta_dir = Path("delta")
        self._last_report_time: datetime | None = None

    def report_fm_accuracy(self, fm_data_path: str, suffix: str = "") -> None:
        """Aggregate FM accuracy metrics from the FM distinct users table."""
        fm_path = self._delta_dir / f"{fm_data_path}{suffix}"
        
        if not fm_path.exists() or not list(fm_path.glob("*.parquet")):
            print(f"[Accuracy] No FM data found at {fm_path}, skipping FM accuracy report")
            return

        try:
            fm_df = pd.read_parquet(fm_path)
            fm_df["window_start"] = pd.to_datetime(fm_df["window_start"])

            # Filter to windows with exact counts (small windows)
            fm_with_exact = fm_df[fm_df["distinct_users_exact"].notna()].copy()
            print(
                f"[Accuracy] FM data loaded from {fm_path}: "
                f"total_windows={len(fm_df)}, "
                f"windows_with_exact={len(fm_with_exact)}"
            )
            estimate_col = (
                "raw_distinct_users_estimate"
                if "raw_distinct_users_estimate" in fm_with_exact.columns
                else "distinct_users_estimate"
            )
            
            if len(fm_with_exact) < self.config.fm_min_windows_for_report:
                print(
                    f"[Accuracy] Only {len(fm_with_exact)} windows with exact counts, "
                    f"need {self.config.fm_min_windows_for_report} for report"
                )
                return

            # Calculate aggregated accuracy metrics
            fm_with_exact["abs_error"] = abs(
                fm_with_exact[estimate_col] - fm_with_exact["distinct_users_exact"]
            )
            fm_with_exact["rel_error"] = (
                fm_with_exact["abs_error"] / fm_with_exact["distinct_users_exact"]
            )

            summary = {
                "report_timestamp": datetime.now(),
                "data_source": f"{fm_data_path}{suffix}",
                "total_windows": len(fm_df),
                "windows_with_exact": len(fm_with_exact),
                "avg_estimate": fm_with_exact[estimate_col].mean(),
                "avg_exact": fm_with_exact["distinct_users_exact"].mean(),
                "avg_abs_error": fm_with_exact["abs_error"].mean(),
                "avg_rel_error": fm_with_exact["rel_error"].mean(),
                "median_rel_error": fm_with_exact["rel_error"].median(),
                "p95_rel_error": fm_with_exact["rel_error"].quantile(0.95),
                "p99_rel_error": fm_with_exact["rel_error"].quantile(0.99),
                "max_rel_error": fm_with_exact["rel_error"].max(),
                "min_rel_error": fm_with_exact["rel_error"].min(),
            }

            summary_df = pd.DataFrame([summary])
            sink_path = f"{self.config.fm_accuracy_sink}{suffix}"
            self.spark.createDataFrame(summary_df).write.mode("append").parquet(sink_path)

            print(
                f"[Accuracy] FM accuracy report: {summary['windows_with_exact']} windows, "
                f"avg rel_error={summary['avg_rel_error']:.2%}, "
                f"median={summary['median_rel_error']:.2%}"
            )

        except Exception as e:
            print(f"[Accuracy] Error reporting FM accuracy: {e}")

    def report_bloom_accuracy(
        self,
        bloom_dup_path: str,
        bloom_metrics_path: str,
        bronze_path: str,
        suffix: str = "",
        sample_windows: int | None = None,
    ) -> None:
        """Analyze Bloom filter false positives/negatives by comparing with exact duplicates."""
        bloom_dup_dir = self._delta_dir / f"{bloom_dup_path}{suffix}"
        bloom_metrics_dir = self._delta_dir / f"{bloom_metrics_path}{suffix}"
        bronze_dir = self._delta_dir / bronze_path

        dup_files = (
            sorted(bloom_dup_dir.glob("*.parquet")) if bloom_dup_dir.exists() else []
        )
        metrics_files = (
            sorted(bloom_metrics_dir.glob("*.parquet")) if bloom_metrics_dir.exists() else []
        )
        if not metrics_files:
            print(
                f"[Accuracy] Missing Bloom data (dup={bool(dup_files)}, "
                f"metrics={bool(metrics_files)}), skipping Bloom accuracy report"
            )
            return

        try:
            # Load Bloom duplicate data
            if dup_files:
                bloom_dup = pd.concat(
                    pd.read_parquet(p) for p in dup_files[-self.config.bloom_sample_files :]
                )
            else:
                bloom_dup = pd.DataFrame(columns=["event_time", "review_id", "window_start"])
            if "event_time" in bloom_dup.columns:
                bloom_dup["window_start"] = pd.to_datetime(bloom_dup["event_time"]).dt.floor("min")
            else:
                bloom_dup["window_start"] = pd.to_datetime([])

            # Load Bloom metrics
            bloom_metrics = pd.concat(
                pd.read_parquet(p) for p in metrics_files[-self.config.bloom_sample_files :]
            )
            bloom_metrics["window_start"] = pd.to_datetime(bloom_metrics["window_start"])

            # Get windows with Bloom duplicates
            windows_with_dups = sorted(bloom_dup["window_start"].unique())

            # Sample windows for analysis (to avoid loading too much bronze data)
            min_windows_needed = self.config.bloom_min_windows_for_report
            effective_sample = sample_windows or self.config.bloom_sample_windows
            sample_size = min(effective_sample, len(windows_with_dups))
            sampled_windows = windows_with_dups[-sample_size:]

            # Load bronze data for sampled windows
            bronze_files = sorted(bronze_dir.rglob("*.parquet"))
            if not bronze_files:
                print(f"[Accuracy] No bronze files found at {bronze_dir}")
                return

            # Load recent bronze files
            bronze_sample = pd.concat(
                pd.read_parquet(p)
                for p in bronze_files[-min(self.config.bloom_sample_files, len(bronze_files)) :]
            )
            bronze_sample["event_time"] = pd.to_datetime(bronze_sample["event_time"])
            bronze_sample["window_start"] = bronze_sample["event_time"].dt.floor("min")

            # Analyze each sampled window
            results = []
            for window_start in sampled_windows:
                window_dt = pd.to_datetime(window_start)
                
                # Get Bloom-flagged duplicates for this window
                bloom_window_dups = bloom_dup[bloom_dup["window_start"] == window_dt]
                bloom_ids = set(bloom_window_dups["review_id"])

                # Get bronze data for this window
                bronze_window = bronze_sample[
                    bronze_sample["window_start"] == window_dt
                ].copy()

                if bronze_window.empty:
                    continue

                # Compute exact duplicates (only repeat occurrences can be flagged)
                bronze_window = bronze_window.sort_values("event_time").copy()
                bronze_window["norm_key"] = (
                    bronze_window["user_id"].fillna("")
                    + "||"
                    + bronze_window["business_id"].fillna("")
                    + "||"
                    + bronze_window["text"]
                    .fillna("")
                    .str.lower()
                    .str.replace(r"\s+", " ", regex=True)
                    .str.strip()
                )
                bronze_window["is_repeat"] = bronze_window["norm_key"].duplicated(keep="first")
                repeat_rows = bronze_window[bronze_window["is_repeat"]]
                if repeat_rows.empty:
                    continue
                exact_ids = set(repeat_rows["review_id"])

                # Calculate metrics
                true_positives = len(bloom_ids & exact_ids)
                false_positives = len(bloom_ids - exact_ids)
                false_negatives = len(exact_ids - bloom_ids)

                precision = true_positives / len(bloom_ids) if bloom_ids else 0.0
                recall = true_positives / len(exact_ids) if exact_ids else 0.0
                f1 = (
                    2 * precision * recall / (precision + recall)
                    if (precision + recall) > 0
                    else 0.0
                )

                results.append({
                    "window_start": window_dt,
                    "bloom_flagged": len(bloom_ids),
                    "exact_duplicates": len(exact_ids),
                    "true_positives": true_positives,
                    "false_positives": false_positives,
                    "false_negatives": false_negatives,
                    "precision": precision,
                    "recall": recall,
                    "f1_score": f1,
                })

            if not results:
                print("[Accuracy] No windows analyzed for Bloom accuracy")
                summary = {
                    "report_timestamp": datetime.now(),
                    "data_source": f"{bloom_dup_path}{suffix}",
                    "windows_analyzed": 0,
                    "avg_bloom_flagged": 0.0,
                    "avg_exact_duplicates": 0.0,
                    "avg_true_positives": 0.0,
                    "avg_false_positives": 0.0,
                    "avg_false_negatives": 0.0,
                    "avg_precision": 0.0,
                    "avg_recall": 0.0,
                    "avg_f1_score": 0.0,
                    "median_precision": 0.0,
                    "median_recall": 0.0,
                }
            else:
                results_df = pd.DataFrame(results)

                if len(results_df) < min_windows_needed:
                    print(
                        f"[Accuracy] Only {len(results_df)} Bloom windows analyzed "
                        f"(need {min_windows_needed} for stable stats)"
                    )

                # Aggregate summary
                summary = {
                    "report_timestamp": datetime.now(),
                    "data_source": f"{bloom_dup_path}{suffix}",
                    "windows_analyzed": len(results_df),
                    "avg_bloom_flagged": results_df["bloom_flagged"].mean(),
                    "avg_exact_duplicates": results_df["exact_duplicates"].mean(),
                    "avg_true_positives": results_df["true_positives"].mean(),
                    "avg_false_positives": results_df["false_positives"].mean(),
                    "avg_false_negatives": results_df["false_negatives"].mean(),
                    "avg_precision": results_df["precision"].mean(),
                    "avg_recall": results_df["recall"].mean(),
                    "avg_f1_score": results_df["f1_score"].mean(),
                    "median_precision": results_df["precision"].median(),
                    "median_recall": results_df["recall"].median(),
                }

            summary_df = pd.DataFrame([summary])
            sink_path = f"{self.config.bloom_accuracy_sink}{suffix}"
            self.spark.createDataFrame(summary_df).write.mode("append").parquet(sink_path)

            print(
                f"[Accuracy] Bloom accuracy report: {summary['windows_analyzed']} windows, "
                f"avg precision={summary['avg_precision']:.2%}, "
                f"avg recall={summary['avg_recall']:.2%}, "
                f"avg FP={summary['avg_false_positives']:.1f}, "
                f"avg FN={summary['avg_false_negatives']:.1f}"
            )

        except Exception as e:
            print(f"[Accuracy] Error reporting Bloom accuracy: {e}")

    def generate_all_reports(self, suffix: str = "") -> None:
        """Generate accuracy reports for both FM and Bloom."""
        # FM accuracy report
        self.report_fm_accuracy("fm_distinct_users", suffix=suffix)

        # Bloom accuracy report
        self.report_bloom_accuracy(
            "dup_bloom",
            "gold_dup_rate_1m",
            "bronze_reviews",
            suffix=suffix,
        )

