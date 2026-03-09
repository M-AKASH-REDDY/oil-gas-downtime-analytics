# Architecture

## Diagram
- Source file: `docs/architecture-diagram.mmd`

## Overview
- `data_gen`: emits synthetic telemetry and maintenance/failure logs.
- `infra`: Docker Compose stack for Kafka, Zookeeper, Postgres, and optional Spark.
- `pipelines/streaming`: Spark Structured Streaming reads Kafka `telemetry`, writes Bronze and Silver parquet datasets.
- `pipelines/batch`: joins Silver telemetry with maintenance/failure data to compute Gold KPIs and load them into Postgres.
- `dq`: data quality checks for Silver and Gold.
- `api`: FastAPI service serving KPI data from Postgres.
- `analytics`: SQL examples and Streamlit dashboard (reads API, with parquet fallback).
- `infra/docker-compose.deploy.yml`: deploy-ready stack (`postgres -> bootstrap -> api -> dashboard`).

## Medallion Layers
- Bronze: append-only raw events from Kafka.
- Silver: cleaned telemetry with dedupe and timestamp normalization.
- Gold: daily KPI facts by asset (`MTBF`, `MTTR`, `downtime_minutes`, `availability_pct`, `production_loss_bbl`).
