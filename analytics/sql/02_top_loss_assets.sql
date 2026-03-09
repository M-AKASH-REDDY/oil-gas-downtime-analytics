SELECT
  asset_id,
  SUM(production_loss_bbl) AS total_loss_bbl,
  AVG(availability_pct) AS avg_availability_pct
FROM kpis_by_asset_day
GROUP BY asset_id
ORDER BY total_loss_bbl DESC
LIMIT 10;

