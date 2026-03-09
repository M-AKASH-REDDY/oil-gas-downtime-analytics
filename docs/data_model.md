# Data Model

## Bronze `telemetry`
- `asset_id` string
- `event_ts` string/timestamp
- `temperature_c` double
- `vibration_mm_s` double
- `pressure_kpa` double
- `production_rate_bbl_hr` double
- `status` string
- `unit_temperature` string
- `unit_pressure` string

## Silver `telemetry_clean`
- Same columns as Bronze with:
  - parsed timestamp
  - invalid pressure nulling
  - dedupe on (`asset_id`, `event_ts`)

## Gold `kpis_by_asset_day`
- `asset_id` string
- `event_date` date
- `mtbf_minutes` double
- `mttr_minutes` double
- `downtime_minutes` double
- `availability_pct` double (0..100)
- `production_loss_bbl` double
- `repair_count` int
- `failure_count` int
- `total_events` int

