from __future__ import annotations

import pandas as pd

from dq.run_checks import validate_gold, validate_silver


def test_validate_silver_duplicate_detection() -> None:
    df = pd.DataFrame(
        [
            {"asset_id": "A1", "event_ts": "2026-02-15T00:00:00Z"},
            {"asset_id": "A1", "event_ts": "2026-02-15T00:00:00Z"},
        ]
    )
    errors = validate_silver(df)
    assert any("duplicate" in err.lower() for err in errors)


def test_validate_gold_availability_range() -> None:
    df = pd.DataFrame([{"asset_id": "A1", "availability_pct": 105.0}])
    errors = validate_gold(df)
    assert any("availability_pct" in err for err in errors)

