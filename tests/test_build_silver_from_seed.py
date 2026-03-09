from __future__ import annotations

import pandas as pd

from pipelines.batch.build_silver_from_seed import build_silver_from_seed_df


def test_build_silver_from_seed_df_cleans_and_dedupes() -> None:
    df = pd.DataFrame(
        [
            {
                "asset_id": "A1",
                "event_ts": "2026-02-15T00:00:00Z",
                "temperature_c": None,
                "pressure_kpa": -10.0,
            },
            {
                "asset_id": "A1",
                "event_ts": "2026-02-15T00:00:00Z",
                "temperature_c": 80.0,
                "pressure_kpa": 1000.0,
            },
        ]
    )

    out = build_silver_from_seed_df(df)

    assert len(out) == 1
    assert pd.isna(out.loc[0, "pressure_kpa"])
    assert out.loc[0, "temperature_c"] == 0.0
