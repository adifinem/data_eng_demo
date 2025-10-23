import os, json, time, requests
from fastapi import FastAPI
from kafka import KafkaProducer
from statsd import StatsClient
from service import Ingestor as HapiIngestor, load_yaml

API_BASE = os.getenv("API_BASE", "http://api:8000")
BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9093")
TOPIC = os.getenv("KAFKA_TOPIC", "healthcheck.raw")
STATSD_HOST = os.getenv("STATSD_HOST", "statsd-exporter")
STATSD_PORT = int(os.getenv("STATSD_PORT", "9125"))
STATSD_PREFIX = os.getenv("STATSD_PREFIX", "pipeline.ingestor")
EVENT_LOG = os.getenv("PIPELINE_EVENT_LOG")
EVENT_SOURCE = os.getenv("PIPELINE_EVENT_SOURCE", "ingestor")
HAPI_CONFIG_PATH = os.getenv("INGESTOR_CONFIG", "/app/datasets.yml")

app = FastAPI(title="Ingestor")

# create producer once
producer = KafkaProducer(
    bootstrap_servers=BOOT,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)

stats = StatsClient(host=STATSD_HOST, port=STATSD_PORT, prefix=STATSD_PREFIX)
_hapi_ingestor = None


def log_event(message: str, *, status: str | None = None, source: str | None = None):
    if not EVENT_LOG:
        return
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    tag = f"{status} " if status else ""
    who = source or EVENT_SOURCE
    line = f"{ts} {who} {tag}{message}\n"
    try:
        with open(EVENT_LOG, "a", encoding="utf-8") as fh:
            fh.write(line)
    except Exception:
        pass


def get_hapi_ingestor() -> HapiIngestor:
    global _hapi_ingestor
    if _hapi_ingestor is None:
        cfg = load_yaml(HAPI_CONFIG_PATH)

        def event_logger(msg: str, status: str | None = None):
            log_event(msg, status=status, source="ingestor-hapi")

        _hapi_ingestor = HapiIngestor(cfg, event_logger=event_logger)
    return _hapi_ingestor


@app.get("/run")
def run_ingest():
    stats.incr("run.attempts")
    log_event("trigger received", status="start")
    t0 = time.time()
    try:
        r = requests.get(f"{API_BASE}/health", timeout=5)
        r.raise_for_status()
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": "api/health",
            "data": r.json(),
        }
        producer.send(TOPIC, key=payload["ts"], value=payload)
        producer.flush()
        latency = int((time.time() - t0) * 1000)
        stats.timing("run.latency_ms", latency)
        stats.incr("run.success")
        log_event(f"published message to {TOPIC} latency_ms={latency}", status="success")
        return {"status": "ok", "topic": TOPIC, "latency_ms": latency}
    except Exception as exc:
        stats.incr("run.failure")
        log_event(f"failure: {exc}", status="failure")
        raise


@app.post("/harvest")
def harvest_datasets():
    stats.incr("harvest.attempts")
    log_event("hapi harvest triggered", status="start")
    t0 = time.time()
    try:
        ing = get_hapi_ingestor()
        summaries = ing.run_cycle()
        duration = int((time.time() - t0) * 1000)
        stats.timing("harvest.duration_ms", duration)
        errors = [s for s in summaries if s.get("error")]
        if errors:
            stats.incr("harvest.failure")
            log_event(f"hapi harvest completed with errors; duration_ms={duration}", status="failure")
        else:
            stats.incr("harvest.success")
            total_records = sum(s.get("records", 0) for s in summaries)
            log_event(f"hapi harvest success records={total_records} duration_ms={duration}", status="success")
        return {"status": "ok", "duration_ms": duration, "datasets": summaries}
    except Exception as exc:
        stats.incr("harvest.failure")
        log_event(f"hapi harvest exception: {exc}", status="failure")
        raise
