from __future__ import annotations

import re
from datetime import date

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config import settings


app = FastAPI(title="Oil & Gas KPI API", version="1.0.0")

_TABLE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _table_name() -> str:
    table = settings.postgres_table
    if not _TABLE_PATTERN.match(table):
        raise HTTPException(status_code=500, detail="Invalid POSTGRES_TABLE name.")
    return table


def _engine():
    return create_engine(settings.postgres_url, pool_pre_ping=True)


@app.get("/health")
def health() -> dict[str, str]:
    try:
        engine = _engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


@app.get("/kpis")
def kpis(
    limit: int = Query(default=100, ge=1, le=5000),
    asset_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, object]:
    table = _table_name()
    where_clauses: list[str] = []
    params: dict[str, object] = {"limit": limit}
    if asset_id:
        where_clauses.append("asset_id = :asset_id")
        params["asset_id"] = asset_id
    if start_date:
        where_clauses.append("event_date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        where_clauses.append("event_date <= :end_date")
        params["end_date"] = end_date

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    query = text(
        f"""
        SELECT
          asset_id,
          event_date,
          mtbf_minutes,
          mttr_minutes,
          downtime_minutes,
          availability_pct,
          production_loss_bbl,
          repair_count,
          failure_count,
          total_events
        FROM {table}
        {where_sql}
        ORDER BY event_date DESC, asset_id
        LIMIT :limit
        """
    )

    try:
        engine = _engine()
        with engine.connect() as conn:
            rows = [dict(row._mapping) for row in conn.execute(query, params)]
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=500, detail=f"query failed: {exc}") from exc

    for row in rows:
        if isinstance(row.get("event_date"), date):
            row["event_date"] = row["event_date"].isoformat()
    return {"count": len(rows), "items": rows}


@app.get("/kpis/summary")
def kpi_summary() -> dict[str, float]:
    table = _table_name()
    query = text(
        f"""
        SELECT
          COALESCE(SUM(production_loss_bbl), 0) AS total_production_loss_bbl,
          COALESCE(AVG(availability_pct), 0) AS avg_availability_pct,
          COALESCE(SUM(downtime_minutes), 0) AS total_downtime_minutes
        FROM {table}
        """
    )
    try:
        engine = _engine()
        with engine.connect() as conn:
            row = conn.execute(query).first()
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=500, detail=f"query failed: {exc}") from exc

    if row is None:
        return {
            "total_production_loss_bbl": 0.0,
            "avg_availability_pct": 0.0,
            "total_downtime_minutes": 0.0,
        }

    values = dict(row._mapping)
    return {
        "total_production_loss_bbl": round(float(values["total_production_loss_bbl"]), 2),
        "avg_availability_pct": round(float(values["avg_availability_pct"]), 2),
        "total_downtime_minutes": round(float(values["total_downtime_minutes"]), 2),
    }
