SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f docker/docker-compose.yml
BATCH_ID ?= demo-batch
BATCH_ROOT ?= data/batches
COUNT ?= 10
TOPIC ?= parcel.events
BUNDLE_TARGET ?= free
CATALOG ?= outfordelivery
MANIFEST ?= $(BATCH_ROOT)/$(BATCH_ID)/manifest.json
DATABRICKS_PROFILE ?=
EXPORT_ID ?= $(BATCH_ID)
EXPORT_ROOT ?= data/exports
PIPELINE_JOB_ID ?=

.DEFAULT_GOAL := help

.PHONY: help init up down ps logs console create-topics topics list-topics validate simulate produce generate-replay export-redpanda-batch verify-batch weather stream silver scan-state upload-parcels upload-batch bundle-validate bundle-deploy pipeline-run sync-snowflake resume-snowflake load-snowflake cleanup-export dbt-snowflake test lint clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

init: ## Create .env from .env.example if it does not exist
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example (edit it as needed)")

up: init ## Start the local stack (Redpanda, Console, MinIO, DynamoDB Local, Spark)
	$(COMPOSE) up -d

down: ## Stop the stack, keeping data volumes
	$(COMPOSE) down

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Tail logs (Ctrl-C to stop)
	$(COMPOSE) logs -f

console: ## Print the local UI URLs
	@echo "Redpanda Console: http://localhost:8080"
	@echo "MinIO Console:    http://localhost:9001"

create-topics: ## Create parcel.events and weather topics (rpk, inside the container)
	$(COMPOSE) exec -T redpanda rpk topic create parcel.events -p 6 -r 1 || true
	$(COMPOSE) exec -T redpanda rpk topic create weather -p 1 -r 1 || true
	$(COMPOSE) exec -T redpanda rpk topic list

topics: ## Create all topics and set Schema Registry compatibility (cross-platform)
	python scripts/manage_topics.py

list-topics: ## List Kafka topics
	$(COMPOSE) exec -T redpanda rpk topic list

validate: ## Validate the Avro schemas (no services needed)
	python scripts/validate_schemas.py

simulate: ## Dry-run the simulator (prints sample events, no Kafka)
	python -m simulator.simulate --count 8

produce: ## Stream live Avro events to Kafka, keyed by parcel_id (Ctrl-C to stop)
	python -m simulator.simulate --produce

generate-replay: ## Generate a completed offline batch (set BATCH_ID, COUNT; never overwrites)
	python -m simulator.simulate --sink files --out "$(BATCH_ROOT)" --batch-id "$(BATCH_ID)" --count "$(COUNT)"

export-redpanda-batch: ## Export actual streamed records with saved context (reuse BATCH_ID on retry)
	python scripts/export_redpanda_batch.py --out "$(BATCH_ROOT)" --batch-id "$(BATCH_ID)" --topic "$(TOPIC)"

verify-batch: ## Verify completion, checksums, clock, dimensions and weather for BATCH_ID
	python -m simulator.simulate --verify-batch "$(BATCH_ROOT)/$(BATCH_ID)"

weather: ## Fetch current weather per hub (Open-Meteo, synthetic fallback)
	python -m simulator.weather

stream: ## Run the Spark streaming job inside the spark container (Ctrl-C to stop)
	$(COMPOSE) exec spark-master bash /opt/outfordelivery/streaming/run.sh

silver: ## Promote Bronze Delta to local Silver Parquet inside the spark container
	$(COMPOSE) exec spark-master bash /opt/outfordelivery/streaming/run_silver.sh

scan-state: ## Print a summary of current parcel state from DynamoDB Local
	python scripts/scan_state.py

upload-parcels: ## Legacy compatible-storage dimensions upload; use upload-batch for free
	databricks fs cp data/parcels.jsonl dbfs:/Volumes/$(CATALOG)/ops/raw/parcels/parcels-$$(date +%Y%m%d%H%M%S).jsonl $(if $(DATABRICKS_PROFILE),--profile "$(DATABRICKS_PROFILE)")

upload-batch: ## Upload an existing completed MANIFEST with remote verification; never generates input
	python scripts/upload_batch.py --manifest "$(MANIFEST)" --catalog "$(CATALOG)" $(if $(DATABRICKS_PROFILE),--profile "$(DATABRICKS_PROFILE)")

bundle-validate: ## Validate the Databricks bundle (BUNDLE_TARGET defaults to free)
	databricks bundle validate -t "$(BUNDLE_TARGET)" $(if $(DATABRICKS_PROFILE),--profile "$(DATABRICKS_PROFILE)")

bundle-deploy: ## Deploy the selected bundle target after the deployment review
	databricks bundle deploy -t "$(BUNDLE_TARGET)" $(if $(DATABRICKS_PROFILE),--profile "$(DATABRICKS_PROFILE)")

pipeline-run: ## Run Databricks only for BATCH_ID, saving the managed export as EXPORT_ID
	databricks bundle run outfordelivery_pipeline -t "$(BUNDLE_TARGET)" --params "batch_id=$(BATCH_ID),export_id=$(EXPORT_ID)" $(if $(DATABRICKS_PROFILE),--profile "$(DATABRICKS_PROFILE)")

sync-snowflake: ## Upload MANIFEST, process/export, retain on device, load native Snowflake and build dbt
	python scripts/sync_snowflake.py sync --manifest "$(MANIFEST)" --export-id "$(EXPORT_ID)" --exports-dir "$(EXPORT_ROOT)" --job-id "$(PIPELINE_JOB_ID)" --catalog "$(CATALOG)" $(if $(DATABRICKS_PROFILE),--profile "$(DATABRICKS_PROFILE)")

resume-snowflake: ## Resume the saved run or local export; never generates another batch
	python scripts/sync_snowflake.py resume --export-id "$(EXPORT_ID)" --exports-dir "$(EXPORT_ROOT)" $(if $(DATABRICKS_PROFILE),--profile "$(DATABRICKS_PROFILE)")

load-snowflake: ## Load a verified retained export and run dbt without contacting Databricks
	python scripts/sync_snowflake.py load --export-id "$(EXPORT_ID)" --exports-dir "$(EXPORT_ROOT)"

cleanup-export: ## Explicitly remove EXPORT_ID, retaining another verified local copy
	python scripts/sync_snowflake.py cleanup --export-id "$(EXPORT_ID)" --exports-dir "$(EXPORT_ROOT)"

dbt-snowflake: ## Run dbt build against Snowflake (needs SNOWFLAKE_ACCOUNT and SNOWFLAKE_PRIVATE_KEY_PATH)
	python scripts/run_dbt.py build --target snowflake

test: ## Run unit tests
	pytest -q

lint: ## Lint Python with ruff
	ruff check .

clean: ## Stop the stack and DELETE data volumes
	$(COMPOSE) down -v
