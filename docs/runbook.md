# Runbook

## Bootstrap
1. Create virtual env and install dependencies.
2. Copy `.env.example` to `.env`.
3. Start local infra with Docker Compose.

## Execution order
1. `make up`
2. `make gen`
3. `make silver-local`
4. `make batch`
5. `make dq`
6. `make load-db`
7. `make test`
8. `make api`
9. `make dashboard`

## Deploy run (containerized)
1. `cp .env.example .env`
2. `make deploy-up`
3. Check API health: `http://localhost:4000/health`
4. Open dashboard: `http://localhost:8501`
5. `make deploy-logs` to inspect service status
6. `make deploy-down` to stop and remove stack

## Optional streaming execution
1. In terminal A: `python -m data_gen.generate_data --mode stream`
2. In terminal B: `make stream`
3. Stop both after a few minutes, then run `make batch` and `make dq`.

## Recovery
- If streaming checkpoints are stale, remove `checkpoints/`.
- If output data is stale, remove `data/` and re-run generation.
- If deploy bootstrap fails, check `oilgas-bootstrap` logs and rerun `make deploy-up`.
