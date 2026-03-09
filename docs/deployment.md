# Deployment Guide

## Services
- `postgres`: stores `kpis_by_asset_day`.
- `bootstrap`: one-time job that generates seed data, builds Silver/Gold, runs DQ, loads Postgres.
- `api`: FastAPI service (`/health`, `/kpis`, `/kpis/summary`).
- `dashboard`: Streamlit UI backed by API.

## Start
```bash
cp .env.example .env
docker compose -f infra/docker-compose.deploy.yml up --build -d
```

## Validate
- API health: `http://localhost:4000/health`
- API docs: `http://localhost:4000/docs`
- Dashboard: `http://localhost:8501`

## Stop
```bash
docker compose -f infra/docker-compose.deploy.yml down -v
```
