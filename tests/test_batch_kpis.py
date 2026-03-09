from __future__ import annotations

import pandas as pd

from pipelines.batch.compute_gold import compute_kpis


def test_compute_kpis_basic() -> None:
    silver = pd.DataFrame(
        [
            {
                "asset_id": "ASSET_001",
                "event_ts": "2026-02-15T00:00:00Z",
                "status": "RUNNING",
                "production_rate_bbl_hr": 100.0,
            },
            {
                "asset_id": "ASSET_001",
                "event_ts": "2026-02-15T01:00:00Z",
                "status": "DOWN",
                "production_rate_bbl_hr": 100.0,
            },
        ]
    )
    silver["event_ts"] = pd.to_datetime(silver["event_ts"], utc=True)

    wo = pd.DataFrame(
        [
            {
                "work_order_id": "WO-00001",
                "asset_id": "ASSET_001",
                "start_ts": "2026-02-15T00:30:00Z",
                "end_ts": "2026-02-15T02:30:00Z",
                "downtime_minutes": 120,
            }
        ]
    )
    wo["start_ts"] = pd.to_datetime(wo["start_ts"], utc=True)
    wo["end_ts"] = pd.to_datetime(wo["end_ts"], utc=True)

    failures = pd.DataFrame(
        [{"asset_id": "ASSET_001", "failure_ts": "2026-02-15T00:30:00Z", "failure_type": "MECH"}]
    )
    failures["failure_ts"] = pd.to_datetime(failures["failure_ts"], utc=True)

    out = compute_kpis(silver, wo, failures)
    assert len(out) == 1
    assert out.loc[0, "asset_id"] == "ASSET_001"
    assert out.loc[0, "downtime_minutes"] == 120
    assert 0 <= out.loc[0, "availability_pct"] <= 100

