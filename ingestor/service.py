import os, sys, json, time, signal, logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
import requests
import yaml
from dateutil import parser as dtparse
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type
from kafka import KafkaProducer

SCHEMAS_DIR = "./schemas"
STATE_PATH   = "./state/state.json"

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ingestor")

# ---------- Helpers ----------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_duration_iso8601(s: str) -> timedelta:
    # Very small parser for PT#M, PT#H, P#D etc. Good enough for our config.
    # Accepts forms like PT5M, PT10M, PT1H, P1D
    if not s:
        return timedelta(minutes=5)
    s = s.upper()
    days = hours = minutes = seconds = 0
    if s.startswith("P") and "T" not in s:
        # P#D
        days = int(s[1:-1]) if s.endswith("D") else 0
    else:
        # PT#H#M#S combos; keep it simple
        assert s.startswith("PT") or s.startswith("P"), f"Bad duration: {s}"
        t = s.split("T")[-1]
        num = ""
        for ch in t:
            if ch.isdigit():
                num += ch
            else:
                if ch == "H":
                    hours = int(num or "0"); num = ""
                elif ch == "M":
                    minutes = int(num or "0"); num = ""
                elif ch == "S":
                    seconds = int(num or "0"); num = ""
        if "P" in s and "D" in s:
            # handle P#DT#H#M etc (rare in our config)
            days_part = s.split("T")[0]
            days_num = days_part.replace("P","").replace("D","")
            days = int(days_num or "0")
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"datasets": {}}

def save_state(state: Dict[str, Any]):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, STATE_PATH)

# ---------- HAPI Client ----------
class HAPIError(Exception): pass

class HAPIClient:
    def __init__(self, base_url: str, session: Optional[requests.Session] = None):
        self.base_url = base_url.rstrip("/")
        self.s = session or requests.Session()
        # Session-level retry for transient network issues
        adapter = requests.adapters.HTTPAdapter(
            max_retries=requests.packages.urllib3.util.retry.Retry(
                total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504]
            )
        )
        self.s.mount("http://", adapter)
        self.s.mount("https://", adapter)

    @retry(
        retry=retry_if_exception_type(HAPIError),
        wait=wait_exponential_jitter(initial=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def info(self, dataset_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/info"
        r = self.s.get(url, params={"id": dataset_id}, timeout=30)
        if r.status_code >= 400:
            raise HAPIError(f"info {dataset_id} -> HTTP {r.status_code}: {r.text[:200]}")
        return r.json()

    @retry(
        retry=retry_if_exception_type(HAPIError),
        wait=wait_exponential_jitter(initial=1, max=60),
        stop=stop_after_attempt(6),
        reraise=True,
    )
    def data_rows(self, dataset_id: str, params: List[str], tmin: str, tmax: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/data"
        q = {
            "id": dataset_id,
            "parameters": ",".join(params),
            "time.min": tmin,
            "time.max": tmax,
            "format": "json",
        }
        r = self.s.get(url, params=q, timeout=60)
        if r.status_code >= 400:
            raise HAPIError(f"data {dataset_id} -> HTTP {r.status_code}: {r.text[:200]}")
        payload = r.json()
        data = payload.get("data", [])
        if not data:
            return []
        cols = [p.get("name") for p in payload.get("parameters", [])]
        rows = []
        for entry in data:
            if len(entry) != len(cols):
                continue
            rows.append({cols[i]: entry[i] for i in range(len(cols))})
        return rows

# ---------- Kafka ----------
class Producer:
    def __init__(self, bootstrap: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, separators=(",",":")).encode("utf-8"),
            linger_ms=100,
            retries=5,
            acks="all",
        )

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential_jitter(initial=1, max=20),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def send(self, topic: str, key: str, value: Dict[str, Any]):
        fut = self.producer.send(topic, key=key, value=value)
        fut.get(timeout=30)

    def flush(self):
        self.producer.flush(timeout=10)

# ---------- Ingestor ----------
class Ingestor:
    def __init__(self, cfg: Dict[str, Any], event_logger=None):
        self.cfg = cfg
        self.base_url = cfg["server"]
        self.poll_td = parse_duration_iso8601(cfg.get("poll_interval", "PT5M"))
        self.default_window = parse_duration_iso8601(cfg.get("default_window", "PT10M"))
        self.datasets = cfg.get("datasets", [])
        self.client = HAPIClient(self.base_url)
        self.kfk = Producer(os.getenv("KAFKA_BOOTSTRAP", "kafka:9093"))
        ensure_dir(SCHEMAS_DIR)
        ensure_dir(os.path.dirname(STATE_PATH))
        self.state = load_state()
        self.shutdown = False
        self.event_logger = event_logger

    def set_event_logger(self, fn):
        self.event_logger = fn

    def log_event(self, message: str, status: Optional[str] = None):
        if not self.event_logger:
            if status:
                log.info("%s %s", status, message)
            else:
                log.info(message)
            return
        try:
            self.event_logger(message, status=status)
        except Exception:
            # never break ingestion because of logging
            pass

    def handle_signals(self):
        def _sig(_sig, _frm):
            log.info("Received signal, shutting down gracefully...")
            self.shutdown = True
        signal.signal(signal.SIGTERM, _sig)
        signal.signal(signal.SIGINT, _sig)

    def cache_info(self, dataset_id: str) -> Dict[str, Any]:
        path = os.path.join(SCHEMAS_DIR, f"{dataset_id}.json")
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
        info = self.client.info(dataset_id)
        with open(path, "w") as f:
            json.dump(info, f, indent=2, sort_keys=True)
        log.info(f"Cached /info for {dataset_id}")
        return info

    def get_last_seen(self, dataset_id: str) -> Optional[datetime]:
        ds = self.state["datasets"].get(dataset_id)
        if ds and "last_seen_end_time" in ds:
            try:
                return dtparse.isoparse(ds["last_seen_end_time"])
            except Exception:
                return None
        return None

    def set_last_seen(self, dataset_id: str, dt_: datetime):
        self.state["datasets"].setdefault(dataset_id, {})
        prev = self.state["datasets"][dataset_id].get("last_seen_end_time")
        # only advance
        if not prev or dtparse.isoparse(prev) < dt_:
            self.state["datasets"][dataset_id]["last_seen_end_time"] = to_iso(dt_)
            save_state(self.state)

    def compute_window(self, dataset: Dict[str, Any]):
        window_td = parse_duration_iso8601(dataset.get("window")) if dataset.get("window") else self.default_window
        now = now_utc()
        last_seen = self.get_last_seen(dataset["id"])
        lookback = timedelta(minutes=1)  # small overlap to avoid edge losses
        if last_seen:
            tmin = last_seen - lookback
        else:
            tmin = now - window_td
        tmax = now
        return to_iso(tmin), to_iso(tmax)

    def run_once_dataset(self, ds: Dict[str, Any]) -> Dict[str, Any]:
        dataset_id = ds["id"]
        scope = ds.get("scope", "rt")
        params = ds.get("parameters") or []

        try:
            info = self.cache_info(dataset_id)
        except Exception as e:
            log.warning(f"/info failed for {dataset_id}: {e}")
            self.log_event(f"dataset={dataset_id} cache_info failed: {e}", status="failure")
            return {"dataset_id": dataset_id, "scope": scope, "records": 0, "error": str(e)}

        info_params = [p.get("name") for p in info.get("parameters", []) if p.get("name")]
        info_params = [p for p in info_params if p.lower() != "time"]

        resolved_params: List[str] = []
        if not params:
            resolved_params = info_params
        else:
            lower_map = {p.lower(): p for p in info_params}
            for p in params:
                if p == "*":
                    resolved_params.extend(info_params)
                    continue
                if p.lower() == "time":
                    continue
                resolved_params.append(lower_map.get(p.lower(), p))

        # Ensure 'time' is included first
        params = ["time"] + sorted(set(resolved_params), key=resolved_params.index)

        tmin, tmax = self.compute_window(ds)
        self.log_event(f"dataset={dataset_id} window {tmin}..{tmax}", status="start")

        try:
            rows = self.client.data_rows(dataset_id, params, tmin, tmax)
        except Exception as e:
            log.warning(f"/data failed for {dataset_id}: {e}")
            self.log_event(f"dataset={dataset_id} data fetch failed: {e}", status="failure")
            return {"dataset_id": dataset_id, "scope": scope, "records": 0, "error": str(e)}
        if not rows:
            self.log_event(f"dataset={dataset_id} no rows in window", status="success")
            return {"dataset_id": dataset_id, "scope": scope, "records": 0, "tmin": tmin, "tmax": tmax}

        topic = f"hapi.{dataset_id}.{scope}"
        max_time = None
        sent = 0

        for row in rows:
            tstr = row.get("time") or row.get("Time") or row.get("TimeUTC")
            if not tstr:
                continue
            try:
                tdt = dtparse.isoparse(tstr)
            except Exception:
                # skip malformed
                continue
            # Remove empty strings
            clean_row = {k: v for k, v in row.items() if v not in (None, "", "null")}
            clean_row["time"] = to_iso(tdt)

            payload = {
                "dataset_id": dataset_id,
                "scope": scope,
                "time": clean_row["time"],
                "ingest_ts": to_iso(now_utc()),
                "data": clean_row,
            }
            try:
                self.kfk.send(topic, key=tstr, value=payload)
                sent += 1
                if not max_time or tdt > max_time:
                    max_time = tdt
            except Exception as e:
                log.warning(f"Kafka send failed: {e}")
                break

        self.kfk.flush()
        if max_time:
            self.set_last_seen(dataset_id, max_time)
        summary = {
            "dataset_id": dataset_id,
            "scope": scope,
            "records": sent,
            "topic": topic,
            "tmin": tmin,
            "tmax": tmax,
            "last_time": to_iso(max_time) if max_time else None,
        }
        status = "success" if sent else "success"
        self.log_event(
            f"dataset={dataset_id} sent={sent} topic={topic} last_time={summary['last_time']}",
            status=status,
        )
        return summary

    def run_cycle(self) -> List[Dict[str, Any]]:
        summaries = []
        for ds in self.datasets:
            try:
                summaries.append(self.run_once_dataset(ds) or {})
            except Exception as e:
                log.exception(f"Unhandled error for dataset {ds.get('id')}: {e}")
                self.log_event(f"dataset={ds.get('id')} error {e}", status="failure")
                summaries.append({"dataset_id": ds.get("id"), "error": str(e)})
        return summaries

    def run(self):
        self.handle_signals()
        while not self.shutdown:
            start = time.time()
            self.run_cycle()
            # Sleep until next poll boundary
            if self.shutdown:
                break
            elapsed = time.time() - start
            sleep_s = max(5.0, self.poll_td.total_seconds() - elapsed)
            log.info(f"Sleeping {sleep_s:.1f}s")
            # Interruptible sleep
            end = time.time() + sleep_s
            while time.time() < end:
                if self.shutdown:
                    break
                time.sleep(0.5)

if __name__ == "__main__":
    cfg = load_yaml(os.getenv("INGESTOR_CONFIG", "./datasets.yml"))
    ing = Ingestor(cfg)
    ing.run()
