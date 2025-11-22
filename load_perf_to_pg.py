import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


# Map Delta subdirectories -> Postgres table names
DATASETS: dict[str, str] = {
    # Traffic metrics (all vs high-rated)
    "metrics_1m": "metrics_1m",
    "metrics_1m_high_rated": "metrics_1m_high_rated",
    # Duplicate rate metrics
    "gold_dup_rate_1m": "dup_rate_1m",
    "gold_dup_rate_1m_high_rated": "dup_rate_1m_high_rated",
    # Distinct users (FM)
    "fm_distinct_users": "fm_distinct_users",
    "fm_distinct_users_high_rated": "fm_distinct_users_high_rated",
    # Performance metrics
    "performance_metrics": "performance_metrics",
    "performance_metrics_high_rated": "performance_metrics_high_rated",
    # Storage metrics
    "storage_metrics": "storage_metrics",
    # Accuracy summaries
    "accuracy_fm_summary": "accuracy_fm_summary",
    "accuracy_fm_summary_high_rated": "accuracy_fm_summary_high_rated",
    "accuracy_bloom_summary": "accuracy_bloom_summary",
    "accuracy_bloom_summary_high_rated": "accuracy_bloom_summary_high_rated",
    # LSH bug-summary clusters
    "dup_lsh_bug_summary": "dup_lsh_bug_summary",
}


def load_dataset(delta_dir: Path, subdir: str, table: str, engine) -> None:
    """Load one Delta/Parquet folder into a Postgres table."""
    path = delta_dir / subdir

    # Skip if no data yet
    if not path.exists():
        print(f"[loader] Skipping {subdir}: directory does not exist")
        return
    if not list(path.glob("*.parquet")):
        print(f"[loader] Skipping {subdir}: no .parquet files found")
        return

    print(f"[loader] Reading Parquet from {path} ...")
    df = pd.read_parquet(path)
    if df.empty:
        print(f"[loader] Skipping {subdir}: DataFrame is empty")
        return

    print(f"[loader] Loading {len(df)} rows into table '{table}' ...")
    # If the table doesn't exist, pandas/SQLAlchemy will create it automatically
    df.to_sql(table, engine, if_exists="append", index=False)
    print(f"[loader] Done loading {subdir} -> {table}")


def main() -> None:
    """
    One-off loader to copy key Delta/Parquet datasets into Postgres for Grafana.

    Datasets covered (under ./delta/):
      - metrics_1m, metrics_1m_high_rated
      - gold_dup_rate_1m, gold_dup_rate_1m_high_rated
      - fm_distinct_users, fm_distinct_users_high_rated
      - performance_metrics, performance_metrics_high_rated
      - storage_metrics
      - accuracy_fm_summary (+ _high_rated)
      - accuracy_bloom_summary (+ _high_rated)
      - dup_lsh_bug_summary

    Connection details can be overridden via environment variables:
      - PG_HOST (default: localhost)
      - PG_PORT (default: 5432)
      - PG_DB   (default: metrics_db)
      - PG_USER (default: metrics_user)
      - PG_PASSWORD (default: metrics_pw)
    """
    project_root = Path(__file__).resolve().parent
    delta_dir = project_root / "delta"

    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB", "metrics_db")
    user = os.getenv("PG_USER", "metrics_user")
    password = os.getenv("PG_PASSWORD", "metrics_pw")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    print(f"[loader] Connecting to Postgres at {host}:{port}/{db} as {user} ...")
    engine = create_engine(url)

    for subdir, table in DATASETS.items():
        try:
            load_dataset(delta_dir, subdir, table, engine)
        except Exception as exc:
            print(f"[loader] Error loading {subdir} -> {table}: {exc}")

    print("[loader] All dataset loads attempted.")


if __name__ == "__main__":
    main()



