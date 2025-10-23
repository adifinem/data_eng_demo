import os, json, socket, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.streaming import StreamingQueryListener

TOPIC = "healthcheck.raw"
BOOT  = "kafka:9093"
OUT   = "/datalake/bronze/healthcheck"
CKPT  = "/datalake/checkpoints/healthcheck"
STATSD_HOST = os.getenv("STATSD_HOST", "statsd-exporter")
STATSD_PORT = int(os.getenv("STATSD_PORT", "9125"))
STATSD_PREFIX = os.getenv("STATSD_PREFIX", "pipeline.spark_stream")
EVENT_LOG = os.getenv("PIPELINE_EVENT_LOG")
EVENT_SOURCE = os.getenv("PIPELINE_EVENT_SOURCE", "spark-stream")

schema = StructType([
    StructField("ts",   StringType(), True),
    StructField("source", StringType(), True),
    StructField("data",  MapType(StringType(), StringType()), True),
])

_stats_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_stats_sock.setblocking(False)

def _stats_send(metric: str, value, metric_type: str):
    try:
        msg = f"{STATSD_PREFIX}.{metric}:{value}|{metric_type}"
        _stats_sock.sendto(msg.encode("utf-8"), (STATSD_HOST, STATSD_PORT))
    except Exception:
        # metrics emission should never crash the streaming job
        pass

def stats_incr(metric: str, value: int = 1):
    _stats_send(metric, value, "c")

def stats_gauge(metric: str, value):
    _stats_send(metric, value, "g")

def stats_timing(metric: str, value_ms: int):
    _stats_send(metric, value_ms, "ms")


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


class StatsdListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        stats_incr("query_started_total")
        log_event(f"stream started id={event.id}", status="start")

    def onQueryTerminated(self, event):
        stats_incr("query_terminated_total")
        reason = getattr(event, "exception", None) or "completed"
        status = "failure" if event.exception else "end"
        log_event(f"stream terminated reason={reason}", status=status)

    def onQueryProgress(self, event):
        progress = event.progress
        stats_gauge("last_batch_id", progress.batchId)
        stats_gauge("last_num_input_rows", progress.numInputRows)
        duration = progress.durationMs.get("addBatch") if progress.durationMs else None
        if duration is not None:
            try:
                stats_timing("last_batch_add_ms", int(duration))
            except ValueError:
                pass
        try:
            stats_gauge("processed_rows_total", progress.numInputRows)
        except Exception:
            pass
        if progress.sources:
            try:
                end_offset = progress.sources[0].endOffset
                if isinstance(end_offset, str):
                    offset_obj = json.loads(end_offset)
                else:
                    offset_obj = end_offset
                latest_topic = next(iter(offset_obj.values()))
                if isinstance(latest_topic, dict):
                    latest_partition = next(iter(latest_topic.values()))
                    stats_gauge("last_end_offset", int(latest_partition))
            except Exception:
                pass
        if progress.numInputRows:
            log_event(
                f"processed batch_id={progress.batchId} rows={progress.numInputRows}",
                status="success",
            )

spark = (SparkSession.builder
         .appName("healthcheck_kafka_to_parquet")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
spark.streams.addListener(StatsdListener())

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOT)
      .option("subscribe", TOPIC)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load())

parsed = (df.selectExpr("CAST(key AS STRING) as k", "CAST(value AS STRING) as v")
            .withColumn("json", from_json(col("v"), schema))
            .select(
                col("json.ts").alias("ts"),
                col("json.source").alias("source"),
                col("json.data").alias("data"),
                current_timestamp().alias("ingest_ts"),
            )
            .withColumn("dataset_id", lit("healthcheck")))

# partition by date for easier browsing
from pyspark.sql.functions import to_date
final = parsed.withColumn("dt", to_date(col("ingest_ts")))

(q := final.writeStream
    .format("parquet")
    .option("checkpointLocation", CKPT)
    .option("path", OUT)
    .option("compression", "zstd")
    .partitionBy("dt")
    .outputMode("append")
    .start()).awaitTermination()
