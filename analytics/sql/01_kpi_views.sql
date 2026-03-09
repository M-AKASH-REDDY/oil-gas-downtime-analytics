-- Gold KPI query examples
SELECT
  asset_id,
  event_date,
  mtbf_minutes,
  mttr_minutes,
  downtime_minutes,
  availability_pct,
  production_loss_bbl
FROM kpis_by_asset_day
ORDER BY event_date DESC, asset_id;

