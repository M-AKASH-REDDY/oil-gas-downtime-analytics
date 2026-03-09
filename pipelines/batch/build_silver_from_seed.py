from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.config import settings
from src.logging_utils import setup_logging


LOGGER = logging.getLogger("build_silver_from_seed")


def _read_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Seed telemetry file not found: {path}")
    return pd.read_json(path, lines=True)


def build_silver_from_seed_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["event_ts"] = pd.to_datetime(out["event_ts"], utc=True, errors="coerce")
    out["temperature_c"] = out["temperature_c"].fillna(0.0)
    out["pressure_kpa"] = out["pressure_kpa"].where(out["pressure_kpa"] >= 0)
    out = out.drop_duplicates(subset=["asset_id", "event_ts"]).reset_index(drop=True)
    return out


def main() -> None:
    setup_logging(settings.log_level)
    raw_path = settings.raw_data_dir / "telemetry_seed.jsonl"
    silver_dir = settings.silver_dir
    silver_dir.mkdir(parents=True, exist_ok=True)

    df = _read_jsonl(raw_path)
    silver_df = build_silver_from_seed_df(df)
    out_path = silver_dir / "telemetry_seed_silver.parquet"
    silver_df.to_parquet(out_path, index=False)
    LOGGER.info("Silver parquet written from seed file: %s", out_path)


if __name__ == "__main__":
    main()
