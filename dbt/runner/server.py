import os, subprocess, time
from fastapi import FastAPI
from statsd import StatsClient

app = FastAPI(title="dbt-runner")
PROJECT = os.getenv("DBT_PROJECT_DIR", "/dbt/spaceweather")
STATSD_HOST = os.getenv("STATSD_HOST", "statsd-exporter")
STATSD_PORT = int(os.getenv("STATSD_PORT", "9125"))
STATSD_PREFIX = os.getenv("STATSD_PREFIX", "pipeline.dbt")
EVENT_LOG = os.getenv("PIPELINE_EVENT_LOG")
EVENT_SOURCE = os.getenv("PIPELINE_EVENT_SOURCE", "dbt-runner")
stats = StatsClient(host=STATSD_HOST, port=STATSD_PORT, prefix=STATSD_PREFIX)


def log_event(message: str, *, status: str | None = None):
    if not EVENT_LOG:
        return
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    tag = f"{status} " if status else ""
    try:
        with open(EVENT_LOG, "a", encoding="utf-8") as fh:
            fh.write(f"{ts} {EVENT_SOURCE} {tag}{message}\n")
    except Exception:
        pass


@app.get("/run")
def run():
    stats.incr("run.attempts")
    log_event("dbt run triggered", status="start")
    t0 = time.time()
    cmd = ["dbt", "run", "--project-dir", PROJECT, "--profiles-dir", "/dbt"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    duration = int((time.time() - t0) * 1000)
    stats.timing("run.duration_ms", duration)
    if proc.returncode == 0:
        stats.incr("run.success")
        log_event(f"dbt run succeeded duration_ms={duration}", status="success")
    else:
        stats.incr("run.failure")
        log_event(f"dbt run failed rc={proc.returncode} duration_ms={duration}", status="failure")
    return {"rc": proc.returncode, "stdout": proc.stdout[-800:], "stderr": proc.stderr[-800:]}
