
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, count, lit
import os, sys, time

RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")
CUR_BUCKET = os.getenv("MINIO_BUCKET_CURATED", "curated")
AGG_BUCKET = os.getenv("MINIO_BUCKET_AGG", "aggregated")
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")

def build_spark():
    return (SparkSession.builder
        .appName("batch-curation-aggregation")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", os.getenv("S3A_PATH_STYLE_ACCESS","true"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate())

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Read all new raw objects for today's ingest date
    # In practice: pushdown by ingest partition; here we read entire bucket for simplicity
    raw_path = f"s3a://{RAW_BUCKET}/"
    df = spark.read.format("csv").option("header", "true").load(raw_path)

    # Basic sanity checks (expect a timestamp column; adjust to your schema)
    ts_col = None
    for cand in ["timestamp","ts","time","event_time","date"]:
        if cand in df.columns:
            ts_col = cand
            break

    if ts_col is None:
        # attempt: if not present, synth a timestamp now (not ideal, but lets pipeline run)
        df = df.withColumn("timestamp", lit(None).cast("timestamp"))
        ts_col = "timestamp"

    df = df.withColumn(ts_col, to_timestamp(col(ts_col)))

    # Deduplicate by all columns (adjust key), drop obvious bad rows
    df = df.dropna(how="all").dropDuplicates()

    # Write curated parquet partitioned by ingestion date (simulated via current date)
    curated_path = f"s3a://{CUR_BUCKET}/"
    (df.write.mode("overwrite")
        .format("parquet")
        .save(curated_path))

    # Simple aggregation example: count by day (if timestamp exists)
    if ts_col:
        agg = (df.withColumn("day", col(ts_col).cast("date"))
                 .groupBy("day").count()
                 .orderBy("day"))
        agg_path = f"s3a://{AGG_BUCKET}/snapshot_dt={time.strftime('%Y%m%d')}/"
        (agg.write.mode("overwrite")
            .format("parquet")
            .save(agg_path))

    spark.stop()

if __name__ == "__main__":
    main()
