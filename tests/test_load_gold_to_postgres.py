from __future__ import annotations

import pandas as pd

from pipelines.batch.load_gold_to_postgres import _prepare_for_sql


def test_prepare_for_sql_normalizes_types() -> None:
    df = pd.DataFrame(
        [
            {
                "asset_id": "ASSET_001",
                "event_date": "2026-02-15",
                "mtbf_minutes": "100.5",
                "mttr_minutes": "20.2",
                "downtime_minutes": "30",
                "availability_pct": "99.0",
                "production_loss_bbl": "12.34",
                "repair_count": "2",
                "failure_count": "1",
                "total_events": "10",
            }
        ]
    )

    out = _prepare_for_sql(df)
    assert out.loc[0, "event_date"].isoformat() == "2026-02-15"
    assert float(out.loc[0, "availability_pct"]) == 99.0
    assert int(out.loc[0, "repair_count"]) == 2
