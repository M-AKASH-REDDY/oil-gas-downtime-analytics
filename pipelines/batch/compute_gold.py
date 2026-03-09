from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.config import settings
from src.logging_utils import setup_logging


LOGGER = logging.getLogger("batch")


def _load_silver(path: Path) -> pd.DataFrame:
    files = list(path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No silver parquet files found in {path}")
    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True, errors="coerce")
    return df


def _load_work_orders(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["start_ts"] = pd.to_datetime(df["start_ts"], utc=True, errors="coerce")
    df["end_ts"] = pd.to_datetime(df["end_ts"], utc=True, errors="coerce")
    return df


def _load_failures(path: Path) -> pd.DataFrame:
    df = pd.read_json(path)
    df["failure_ts"] = pd.to_datetime(df["failure_ts"], utc=True, errors="coerce")
    return df


def compute_kpis(silver_df: pd.DataFrame, wo_df: pd.DataFrame, failures_df: pd.DataFrame) -> pd.DataFrame:
    telemetry = silver_df.copy()
    telemetry["event_date"] = telemetry["event_ts"].dt.date
    tele_daily = (
        telemetry.groupby(["asset_id", "event_date"], as_index=False)
        .agg(
            total_events=("event_ts", "count"),
            down_events=("status", lambda x: (x == "DOWN").sum()),
            avg_prod_rate=("production_rate_bbl_hr", "mean"),
        )
    )

    wo = wo_df.copy()
    wo["event_date"] = wo["start_ts"].dt.date
    wo_daily = wo.groupby(["asset_id", "event_date"], as_index=False).agg(
        downtime_minutes=("downtime_minutes", "sum"),
        repair_count=("work_order_id", "count"),
    )

    failures = failures_df.copy()
    failures["event_date"] = failures["failure_ts"].dt.date
    fail_daily = failures.groupby(["asset_id", "event_date"], as_index=False).agg(
        failure_count=("failure_ts", "count")
    )

    gold = tele_daily.merge(wo_daily, on=["asset_id", "event_date"], how="left").merge(
        fail_daily, on=["asset_id", "event_date"], how="left"
    )
    gold[["downtime_minutes", "repair_count", "failure_count"]] = gold[
        ["downtime_minutes", "repair_count", "failure_count"]
    ].fillna(0)

    gold["availability_pct"] = (
        (1 - (gold["downtime_minutes"] / 1440.0)).clip(lower=0, upper=1) * 100.0
    ).round(2)
    gold["mttr_minutes"] = (gold["downtime_minutes"] / gold["repair_count"].replace(0, pd.NA)).fillna(0).round(2)
    gold["mtbf_minutes"] = ((1440 - gold["downtime_minutes"]) / gold["failure_count"].replace(0, pd.NA)).fillna(0).round(2)
    gold["production_loss_bbl"] = (
        (gold["downtime_minutes"] / 60.0) * gold["avg_prod_rate"].fillna(0)
    ).round(2)

    return gold[
        [
            "asset_id",
            "event_date",
            "mtbf_minutes",
            "mttr_minutes",
            "downtime_minutes",
            "availability_pct",
            "production_loss_bbl",
            "repair_count",
            "failure_count",
            "total_events",
        ]
    ]


def main() -> None:
    setup_logging(settings.log_level)
    raw = settings.raw_data_dir
    silver = settings.silver_dir
    gold_dir = settings.gold_dir
    gold_dir.mkdir(parents=True, exist_ok=True)

    silver_df = _load_silver(silver)
    wo_df = _load_work_orders(raw / "maintenance_work_orders.csv")
    failures_df = _load_failures(raw / "failure_events.json")

    kpi_df = compute_kpis(silver_df, wo_df, failures_df)
    output_path = gold_dir / "kpis_by_asset_day.parquet"
    kpi_df.to_parquet(output_path, index=False)

    sample_csv = gold_dir / "kpis_by_asset_day_sample.csv"
    kpi_df.head(20).to_csv(sample_csv, index=False)
    LOGGER.info("Gold KPI table written: %s", output_path)
    LOGGER.info("Sample CSV written: %s", sample_csv)


if __name__ == "__main__":
    main()

