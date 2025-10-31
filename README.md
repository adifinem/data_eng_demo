
I created this in an afternoon. I did utilize GPT-5-codex, but in a very engaged, hands-on way, with heavy guidance and correction. Just less carpel tunnel.

It's a bare bones full stack data ingestion pipeline demo to show the full process and use as demo and template. The documentation below is all LLM generated.

Also spaceweather APIs are pretty cool. HAPI is a nicely standardized and well-documented REST protocol, so is itself a good model of how to make a solid, discoverable public API that's self-documenting and automation friendly. This demo is very rudamentary, the observability configs need tweaks, and it's over-engineered for what it is, to illustrate concepts rather than be practical, but there's a lot of potential hiding in what it's doing.


# Spaceweather Pipeline

A toy spaceweather stack that does two things:

1. **Health check loop** – a FastAPI stub emits a heartbeat message every five minutes so you can confirm the Airflow ⇢ Kafka ⇢ Spark ⇢ dbt ⇢ FastAPI ⇢ Streamlit loop works end to end.
2. **Real HAPI ingest** – the same pipeline polls the KNMI Space Weather HAPI endpoints defined in `ingestor/datasets.yml`, streams them into Kafka (`hapi.*` topics), lands the data in `/datalake/bronze/hapi`, models it with dbt/DuckDB, and surfaces simple charts in Streamlit.

All services publish metrics to Prometheus/Grafana and append human-readable events to `observability/logs/pipeline_events.log`, so you can `tail -f` and watch every hop go by.

## Service inventory

| Service | Purpose | Key files / entrypoints |
| --- | --- | --- |
| `airflow-init`, `airflow-web`, `airflow-scheduler` | Orchestrate both DAGs (`e2e_healthcheck_pipeline` and `hapi_ingest_pipeline`). Each task lifecycle is written to the event log. | `airflow/dags/e2e_healthcheck_pipeline.py`, `airflow/dags/hapi_ingest_pipeline.py` |
| `ingestor` | FastAPI app exposing two endpoints: `/run` (health payload → Kafka) and `/harvest` (HAPI polling). Shares the `service.Ingestor` class with Airflow to fetch `/info` metadata, request `/data`, and push rows to Kafka. | `ingestor/app.py`, `ingestor/service.py`, `ingestor/datasets.yml` |
| `kafka` | Single-node Kafka (KRaft). Auto-topic creation enabled. | `docker-compose.yml` |
| `kafka-setup` | One-shot container that creates `healthcheck.raw` on boot and logs readiness. | `docker-compose.yml` |
| `spark-stream` | Structured Streaming job that tails `healthcheck.raw` and writes `/datalake/bronze/healthcheck`. | `spark/jobs/stream_healthcheck_kafka.py` |
| `spark-hapi-stream` | Structured Streaming job (runs `spark-submit --master local[2]`) consuming all `hapi.*` topics, parsing the JSON payload, and writing `/datalake/bronze/hapi/dataset_id=…/scope=…/dt=…`. Logs every micro-batch. | `spark/jobs/stream_hapi_kafka.py` |
| `dbt-runner` | FastAPI wrapper running `dbt run` for both the health and HAPI models. Emits duration/success/failure metrics + log entries. | `dbt/runner/server.py`, `dbt/spaceweather/models/*` |
| `dbt` | Idle helper container (`tail -f /dev/null`) for manual dbt work. _Currently unused in automated paths._ | `docker-compose.yml` |
| `api` | FastAPI backend exposing `/health`, `/messages`, `/hapi/datasets`, `/hapi/data`, `/sql`. | `api/main.py` |
| `app` | Streamlit UI showing the health table and selectable HAPI charts. | `app/Home.py`, `app/Dockerfile` |
| `statsd-exporter`, `prometheus`, `grafana` | Metrics bridge + storage + dashboard. Grafana ships with a “Pipeline Observability” dashboard covering health and HAPI flows. | `observability/prometheus/prometheus.yml`, `observability/grafana/**` |

Named Docker volumes `datalake` and `warehouse` hold the lakehouse (`/datalake/...`) and DuckDB database (`/warehouse/spaceweather.duckdb`).

## Pipelines at a glance

### Healthcheck loop

```
Airflow e2e_healthcheck_pipeline DAG
    └─ SimpleHttpOperator /run (ingestor/app.py)
          └─ Fetch /health from FastAPI stub + publish to Kafka topic healthcheck.raw
                └─ spark-stream → /datalake/bronze/healthcheck
                      └─ dbt-runner (gold__messages) → DuckDB
                            └─ api/main.py /messages → Streamlit table
```

### HAPI ingest loop

```
Airflow hapi_ingest_pipeline DAG
    └─ SimpleHttpOperator /harvest (ingestor/app.py)
          └─ service.Ingestor
                ├─ GET /info (cache to schemas/<dataset>.json)
                ├─ GET /data (JSON) for the 5 minute window
                └─ Publish rows to Kafka topics hapi.<dataset_id>.<scope>
                      └─ spark-hapi-stream → /datalake/bronze/hapi/dataset_id=…/scope=…/dt=…
                            └─ dbt-runner (silver__hapi_measurements → gold__hapi_timeseries)
                                  └─ api/main.py (/hapi/datasets, /hapi/data) → Streamlit charts
```

Key config: `ingestor/datasets.yml` defines the dataset IDs, default polling cadence, and optional parameter overrides. If a dataset omits parameters or uses the `*` wildcard, the service pulls the authoritative list from `/info`. Fill values (e.g., `-1e30`) are nulled out in the dbt model.

## Unified event log

Every major step appends `<ISO8601 UTC> <source> <status> <message>` to `observability/logs/pipeline_events.log`. Examples:

```
2025-10-23T19:49:10Z ingestor-hapi start dataset=solar_wind_mag_rt window 2025-10-23T19:39:00Z..2025-10-23T19:49:10.24Z
2025-10-23T19:49:12Z ingestor-hapi success dataset=solar_wind_mag_rt sent=5 topic=hapi.solar_wind_mag_rt.rt last_time=2025-10-23T19:46:00Z
2025-10-23T19:51:15Z spark-hapi-stream success processed batch_id=3 rows=2
2025-10-23T19:51:16Z dbt-runner success dbt run succeeded duration_ms=1748
```

Follow it live while you develop:

```bash
$ tail -f observability/logs/pipeline_events.log
```

## Metrics, dashboards, and Kafka visibility

- Prometheus: <http://localhost:9090>
  - Health metrics: `pipeline_ingestor_run_success_total`, `pipeline_spark_stream_last_num_input_rows_gauge`, `pipeline_dbt_run_success_total`
  - HAPI metrics: `pipeline_ingestor_harvest_success_total`, `pipeline_spark_hapi_stream_last_num_input_rows_gauge`
- Grafana: <http://localhost:3000> (`admin` / `admin`). The “Pipeline Observability” dashboard now includes panels for HAPI harvest rate and Spark batch sizes.
- StatsD exporter raw metrics: <http://localhost:9102/metrics>
- Kafka UI (configured for the Docker listener `kafka:9093`): <http://localhost:8080>
- CLI fallbacks:

```bash
# list topics
$ docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9093 --list

# watch HAPI payloads
$ docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9093 --topic hapi.solar_wind_mag_rt.rt --from-beginning --max-messages 5
```

## API surface

| Endpoint | Description |
| --- | --- |
| `GET /health` | Service heartbeat |
| `GET /messages?limit=` | Latest healthcheck rows from `gold__messages` |
| `GET /hapi/datasets` | Available dataset IDs, scopes, and parameter names sourced from `gold__hapi_timeseries` |
| `GET /hapi/data?dataset_id=…&parameter=…&limit=…` | Timeseries rows (value + raw string) from `gold__hapi_timeseries` |
| `GET /sql?q=` | Ad-hoc DuckDB query runner |

The Streamlit UI (<http://localhost:8501>) consumes the same endpoints and plots simple lines for whichever dataset/parameter you pick.

## Running the stack

```bash
# Build the images that bundle Python code
$ docker compose build ingestor dbt-runner api app

# Start everything (Kafka, Spark, Airflow, dbt-runner, API, Streamlit, observability)
$ docker compose up -d

# Follow the event log
$ tail -f observability/logs/pipeline_events.log

# Trigger pipelines from Airflow
$ docker compose exec airflow-webserver airflow dags trigger e2e_healthcheck_pipeline
$ docker compose exec airflow-webserver airflow dags trigger hapi_ingest_pipeline
```

Each Airflow run will invoke `dbt-runner` at the end; you can also call it directly:

```bash
$ curl -s http://localhost:9001/run | jq
```

## Troubleshooting tips

- **Kafka UI shows the cluster offline** – it may take a few seconds after broker startup. Verify the broker manually with `kafka-topics.sh --list`. The UI is pinned to `kafka:9093`.
- **HAPI topic empty** – check `observability/logs/pipeline_events.log` for `ingestor-hapi` entries. If the API returns fill values only, the dbt model will surface `value = null`; check `/hapi/data?...` to confirm.
- **Spark HAPI stream idle** – ensure `spark-hapi-stream` is running (logs should show `processed batch_id=…`). The service uses `--master local[2]`, so it no longer depends on cluster resources.
- **dbt run errors** – `dbt-runner` returns the last 800 characters of stdout/stderr. View them via `curl http://localhost:9001/run` or the event log entries.

## Further evaluation

- The `dbt` container is only for manual CLI access; you can remove it from `docker-compose.yml` if you prefer a leaner stack.
- `solar_wind_mag_ace_science` currently returns no rows in the default 5-minute window; decide whether to expand the window or remove it from `ingestor/datasets.yml`.
- Consider adding Kafka JMX → Prometheus exporters if you need broker-level metrics beyond the simple UI / console commands.

## File map reference

- Airflow DAGs: `airflow/dags/e2e_healthcheck_pipeline.py`, `airflow/dags/hapi_ingest_pipeline.py`
- Ingestor service: `ingestor/app.py`, `ingestor/service.py`, `ingestor/datasets.yml`
- Kafka bootstrapper: `docker-compose.yml` (`kafka`, `kafka-setup` services)
- Spark streaming jobs: `spark/jobs/stream_healthcheck_kafka.py`, `spark/jobs/stream_hapi_kafka.py`
- dbt project: `dbt/spaceweather/models/gold__messages.sql`, `dbt/spaceweather/models/silver__hapi_measurements.sql`, `dbt/spaceweather/models/gold__hapi_timeseries.sql`
- dbt runner API: `dbt/runner/server.py`
- FastAPI backend: `api/main.py`
- Streamlit frontend: `app/Home.py`
- Observability config: `observability/prometheus/prometheus.yml`, `observability/grafana/**`, `observability/logs/pipeline_events.log`

With metrics, dashboards, and the unified event log in place you can watch each service come online at `docker compose up`, then trace every Airflow-triggered harvest from Kafka publish to Spark batch to dbt model, straight through to the Streamlit chart.
