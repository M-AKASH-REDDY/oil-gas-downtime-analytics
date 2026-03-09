SHELL := /bin/bash

PYTHON := python
PIP := pip

.PHONY: install up down deploy-up deploy-down deploy-logs gen stream silver-local batch load-db dq api dashboard test lint clean

install:
	$(PIP) install -r requirements.txt

up:
	docker compose -f infra/docker-compose.yml up -d

down:
	docker compose -f infra/docker-compose.yml down

deploy-up:
	docker compose -f infra/docker-compose.deploy.yml up --build -d

deploy-down:
	docker compose -f infra/docker-compose.deploy.yml down -v

deploy-logs:
	docker compose -f infra/docker-compose.deploy.yml logs -f --tail=200

gen:
	$(PYTHON) -m data_gen.generate_data --mode once

stream:
	$(PYTHON) -m pipelines.streaming.spark_stream

silver-local:
	$(PYTHON) -m pipelines.batch.build_silver_from_seed

batch:
	$(PYTHON) -m pipelines.batch.compute_gold

load-db:
	$(PYTHON) -m pipelines.batch.load_gold_to_postgres

dq:
	$(PYTHON) -m dq.run_checks

api:
	$(PYTHON) -m uvicorn api.main:app --host 0.0.0.0 --port 4000

dashboard:
	$(PYTHON) -m streamlit run analytics/dashboard.py --server.address=0.0.0.0 --server.port=8501

test:
	pytest -q

lint:
	ruff check .

clean:
	rm -rf data checkpoints logs ge_data
