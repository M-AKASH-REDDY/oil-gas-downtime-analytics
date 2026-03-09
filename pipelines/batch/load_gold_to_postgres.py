from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from src.config import settings
from src.logging_utils import setup_logging


LOGGER = logging.getLogger("load_gold_to_postgres")


def _read_gold(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Gold parquet not found: {path}")
    return pd.read_parquet(path)


def _prepare_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["event_date"] = pd.to_datetime(out["event_date"], errors="coerce").dt.date
    numeric_cols = [
        "mtbf_minutes",
        "mttr_minutes",
        "downtime_minutes",
        "availability_pct",
        "production_loss_bbl",
    ]
    int_cols = ["repair_count", "failure_count", "total_events"]
    out[numeric_cols] = out[numeric_cols].apply(pd.to_numeric, errors="coerce")
    out[int_cols] = out[int_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)
    return out


def load_gold_to_postgres(df: pd.DataFrame, table: str | None = None) -> int:
    target_table = table or settings.postgres_table
    cleaned = _prepare_for_sql(df)
    engine = create_engine(settings.postgres_url, pool_pre_ping=True)
    cleaned.to_sql(target_table, engine, if_exists="replace", index=False)
    return len(cleaned)


def main() -> None:
    setup_logging(settings.log_level)
    gold_path = settings.gold_dir / "kpis_by_asset_day.parquet"
    rows = load_gold_to_postgres(_read_gold(gold_path))
    LOGGER.info("Loaded %s KPI rows into Postgres table '%s'.", rows, settings.postgres_table)


if __name__ == "__main__":
    main()
