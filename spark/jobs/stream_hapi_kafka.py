import os, json, socket, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
    to_json,
)
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.streaming import StreamingQueryListener

TOPIC_PATTERN = "hapi.*"
BOOT = "kafka:9093"
OUT = "/datalake/bronze/hapi"
CKPT = "/datalake/checkpoints/hapi"
STATSD_HOST = os.getenv("STATSD_HOST", "statsd-exporter")
STATSD_PORT = int(os.getenv("STATSD_PORT", "9125"))
STATSD_PREFIX = os.getenv("STATSD_PREFIX", "pipeline.spark_hapi_stream")
EVENT_LOG = os.getenv("PIPELINE_EVENT_LOG")
EVENT_SOURCE = os.getenv("PIPELINE_EVENT_SOURCE", "spark-hapi-stream")

schema = StructType(
    [
        StructField("dataset_id", StringType(), True),
        StructField("scope", StringType(), True),
        StructField("time", StringType(), True),
        StructField("ingest_ts", StringType(), True),
        StructField("data", MapType(StringType(), StringType()), True),
    ]
)

_stats_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_stats_sock.setblocking(False)


def _stats_send(metric: str, value, metric_type: str):
    try:
        msg = f"{STATSD_PREFIX}.{metric}:{value}|{metric_type}"
        _stats_sock.sendto(msg.encode("utf-8"), (STATSD_HOST, STATSD_PORT))
    except Exception:
        pass


def stats_incr(metric: str, value: int = 1):
    _stats_send(metric, value, "c")


def stats_gauge(metric: str, value):
    _stats_send(metric, value, "g")


def stats_timing(metric: str, value_ms: int):
    _stats_send(metric, value_ms, "ms")


def log_event(message: str, status: str | None = None):
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
        if progress.numInputRows:
            log_event(
                f"processed batch_id={progress.batchId} rows={progress.numInputRows}",
                status="success",
            )


spark = (
    SparkSession.builder.appName("hapi_kafka_to_parquet").getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.streams.addListener(StatsdListener())

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOT)
    .option("subscribePattern", TOPIC_PATTERN)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    kafka_df.selectExpr("CAST(value AS STRING) as raw")
    .withColumn("json", from_json(col("raw"), schema))
    .select(
        col("json.dataset_id").alias("dataset_id"),
        col("json.scope").alias("scope"),
        to_timestamp(col("json.time")).alias("event_time"),
        to_timestamp(col("json.ingest_ts")).alias("ingest_ts"),
        col("json.data").alias("data_map"),
        to_json(col("json.data")).alias("data_json"),
    )
    .withColumn("dt", to_date(col("event_time")))
)

stream = (
    parsed.writeStream
    .format("parquet")
    .option("checkpointLocation", CKPT)
    .option("path", OUT)
    .option("compression", "zstd")
    .partitionBy("dataset_id", "scope", "dt")
    .outputMode("append")
    .start()
)

stream.awaitTermination()
