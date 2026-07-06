SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f docker/docker-compose.yml

.DEFAULT_GOAL := help

.PHONY: help init up down ps logs console create-topics topics list-topics validate simulate produce weather stream silver scan-state test lint clean

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

weather: ## Fetch current weather per hub (Open-Meteo, synthetic fallback)
	python -m simulator.weather

stream: ## Run the Spark streaming job inside the spark container (Ctrl-C to stop)
	$(COMPOSE) exec spark-master bash /opt/outfordelivery/streaming/run.sh

silver: ## Promote Bronze to Silver (Delta) inside the spark container
	$(COMPOSE) exec spark-master bash /opt/outfordelivery/streaming/run_silver.sh

scan-state: ## Print a summary of current parcel state from DynamoDB Local
	python scripts/scan_state.py

test: ## Run unit tests
	pytest -q

lint: ## Lint Python with ruff
	ruff check .

clean: ## Stop the stack and DELETE data volumes
	$(COMPOSE) down -v
