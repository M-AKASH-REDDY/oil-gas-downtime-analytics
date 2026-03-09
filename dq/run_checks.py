from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from src.config import settings
from src.logging_utils import setup_logging


LOGGER = logging.getLogger("dq")


def _read_parquet_dir(path) -> pd.DataFrame:
    files = list(path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {path}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def validate_silver(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    if df["asset_id"].isna().any():
        errors.append("Silver check failed: null asset_id present.")

    ts = pd.to_datetime(df["event_ts"], utc=True, errors="coerce")
    lower = datetime.now(timezone.utc) - timedelta(days=365)
    upper = datetime.now(timezone.utc) + timedelta(days=1)
    if ((ts < lower) | (ts > upper) | ts.isna()).any():
        errors.append("Silver check failed: event_ts out of expected range.")

    dupes = df.duplicated(subset=["asset_id", "event_ts"]).sum()
    if dupes > 0:
        errors.append(f"Silver check failed: {dupes} duplicate (asset_id, event_ts) rows.")
    return errors


def validate_gold(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    if df["asset_id"].isna().any():
        errors.append("Gold check failed: null asset_id present.")
    if (((df["availability_pct"] < 0) | (df["availability_pct"] > 100)) | df["availability_pct"].isna()).any():
        errors.append("Gold check failed: availability_pct outside [0, 100].")
    return errors


def _run_with_great_expectations(silver_df: pd.DataFrame, gold_df: pd.DataFrame) -> tuple[list[str], str | None]:
    errors: list[str] = []
    try:
        import great_expectations as gx
    except Exception as exc:  # pragma: no cover
        return [], f"Great Expectations import failed: {exc}"

    silver = silver_df.copy()
    silver["event_ts"] = pd.to_datetime(silver["event_ts"], utc=True, errors="coerce")
    silver["event_ts_epoch"] = silver["event_ts"].astype("int64") // 10**9

    lower = int((datetime.now(timezone.utc) - timedelta(days=365)).timestamp())
    upper = int((datetime.now(timezone.utc) + timedelta(days=1)).timestamp())

    try:
        gx_silver = gx.from_pandas(silver)
        gx_gold = gx.from_pandas(gold_df)

        r1 = gx_silver.expect_column_values_to_not_be_null("asset_id")
        r2 = gx_silver.expect_compound_columns_to_be_unique(["asset_id", "event_ts"])
        r3 = gx_silver.expect_column_values_to_be_between("event_ts_epoch", min_value=lower, max_value=upper)
        r4 = gx_gold.expect_column_values_to_not_be_null("asset_id")
        r5 = gx_gold.expect_column_values_to_be_between("availability_pct", min_value=0, max_value=100)
        results = [r1, r2, r3, r4, r5]
        for idx, result in enumerate(results, 1):
            if not bool(result.get("success", False)):
                errors.append(f"GX expectation {idx} failed.")
    except Exception as exc:  # pragma: no cover
        return [], f"Great Expectations runtime failed: {exc}"
    return errors, None


def main() -> None:
    setup_logging(settings.log_level)
    silver_df = _read_parquet_dir(settings.silver_dir)
    gold_df = pd.read_parquet(settings.gold_dir / "kpis_by_asset_day.parquet")

    errors: list[str] = []
    gx_errors, gx_note = _run_with_great_expectations(silver_df, gold_df)
    if gx_note:
        LOGGER.warning("%s. Continuing with deterministic pandas checks.", gx_note)
    errors.extend(gx_errors)
    errors.extend(validate_silver(silver_df))
    errors.extend(validate_gold(gold_df))

    if errors:
        for err in errors:
            LOGGER.error(err)
        raise SystemExit(1)

    LOGGER.info("DQ checks passed for Silver and Gold tables.")


if __name__ == "__main__":
    main()
