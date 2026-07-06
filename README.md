# outfordelivery

A real-time parcel delivery operations pipeline. It simulates parcels moving through a Dutch delivery network, streams their lifecycle events through Kafka, processes them with Spark Structured Streaming into a Delta lakehouse, and serves both a live operations dashboard and a queryable history. It runs end to end on a laptop with Docker at zero cost, and can be deployed to AWS with Terraform.

This is a portfolio project: a built demonstration rather than a production system.

## Tech stack

- Languages: Python 3.11, SQL
- Streaming: Kafka (Redpanda), Schema Registry, Avro
- Processing: Spark Structured Streaming, Delta Lake (Bronze and Silver medallion layers)
- Warehouse: dbt (DuckDB locally, Athena on AWS)
- Serving: DynamoDB, FastAPI
- Frontend: MapLibre GL, Chart.js
- Storage: MinIO locally, S3 on AWS
- Infrastructure: Docker Compose, Terraform, AWS (Glue, S3, DynamoDB, Athena, SNS, Lambda)

## Running it locally

Prerequisites: Docker and Docker Compose, Python 3.11 or newer.

Copy the environment file and start the stack (Redpanda, MinIO, DynamoDB Local, Spark), then create the topics:

```
cp .env.example .env
docker compose --env-file .env -f docker/docker-compose.yml up -d
python scripts/manage_topics.py
```

Create a virtual environment and stream events into Kafka:

```
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m simulator.simulate --produce
```

Process the stream into the Delta lake and the serving store, then build the warehouse:

```
docker compose --env-file .env -f docker/docker-compose.yml exec spark-master bash /opt/outfordelivery/streaming/run.sh
docker compose --env-file .env -f docker/docker-compose.yml exec spark-master bash /opt/outfordelivery/streaming/run_silver.sh
cd dbt && dbt build --profiles-dir . && cd ..
```

Start the dashboard and open http://localhost:8050:

```
uvicorn dashboard.app:app --port 8050
```

## Deploying to AWS (optional)

The `infra/` folder holds the Terraform for a cloud deployment: an S3 data lake, a Redpanda node on EC2, a Glue streaming job, DynamoDB, an Athena workgroup, and SNS and Lambda SLA alerts.

```
cd infra
cp terraform.tfvars.example terraform.tfvars   # set allowed_cidr and alert_email
terraform init
terraform plan
terraform apply
```

Applying creates real, billable resources. `terraform destroy` removes them.

## What the app provides

- Live operations view: current parcel status breakdown, throughput per hub, and a map of active parcels across the Netherlands, refreshed every few seconds from DynamoDB.
- SLA risk tracking: cards flag parcels at risk of breaching their delivery window on a compressed simulation clock. Clicking a card opens the parcels at risk, and clicking a parcel shows its detail.
- History and analytics: a delivery funnel, on-time rate by region, failure analysis, and weather impact, served from the dbt gold marts.

## Configuration

Business and network settings live in `config/`: service levels and SLA windows, failure and reschedule behaviour, the delivery hubs and regions, and the weather source (Open-Meteo, free and keyless). Copy `.env.example` to `.env` for local settings. Secrets are never committed.
