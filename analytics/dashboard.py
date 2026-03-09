from __future__ import annotations

import json
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd
import streamlit as st

from src.config import settings


st.set_page_config(page_title="Oil & Gas Downtime Analytics", layout="wide")
st.title("Oil & Gas Equipment Downtime & Production Loss Analytics")


def _read_kpis_from_api(limit: int = 2000) -> pd.DataFrame | None:
    params = urlencode({"limit": limit})
    url = f"{settings.api_base_url}/kpis?{params}"
    try:
        with urlopen(url, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (URLError, TimeoutError, json.JSONDecodeError):
        return None
    return pd.DataFrame(payload.get("items", []))


def _read_kpis_from_parquet() -> pd.DataFrame | None:
    gold_path = Path(settings.gold_dir) / "kpis_by_asset_day.parquet"
    if not gold_path.exists():
        return None
    return pd.read_parquet(gold_path)


df = _read_kpis_from_api()
if df is None or df.empty:
    df = _read_kpis_from_parquet()

if df is None or df.empty:
    st.warning("No KPI data available from API or local parquet.")
    st.stop()

st.metric("Total Production Loss (bbl)", round(float(df.get("production_loss_bbl", pd.Series([0])).sum()), 2))
st.metric("Average Availability (%)", round(float(df.get("availability_pct", pd.Series([0])).mean()), 2))

st.subheader("KPI Table")
st.dataframe(df.sort_values(["event_date", "asset_id"], ascending=[False, True]), use_container_width=True)
