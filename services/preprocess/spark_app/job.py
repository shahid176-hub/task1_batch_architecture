from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_format, count, sum as _sum, avg, when
)
import os, time

RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")
CUR_BUCKET = os.getenv("MINIO_BUCKET_CURATED", "curated")
AGG_BUCKET = os.getenv("MINIO_BUCKET_AGG", "aggregated")
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")


def build_spark():
    return (
        SparkSession.builder
        .appName("nyc-taxi-curation-aggregation")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_path = f"s3a://{RAW_BUCKET}/"
    df = spark.read.csv(raw_path, header=True, inferSchema=True)

    # ======================== NYC TAXI SCHEMA CLEANING ==========================
    df = (
        df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
          .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    )

    # Remove invalid or corrupted rows
    df = df.filter(col("pickup_datetime").isNotNull())
    df = df.filter(col("trip_distance") >= 0)
    df = df.filter(col("fare_amount") >= 0)
    df = df.filter(col("total_amount") >= 0)

    # Compute duration in minutes
    df = df.withColumn(
        "trip_duration_min",
        (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60
    )

    # Add curated partition (YYYY-MM-DD)
    df = df.withColumn("ingestion_date", date_format(col("pickup_datetime"), "yyyy-MM-dd"))

    # ============================= WRITE CURATED DATA ============================
    curated_path = f"s3a://{CUR_BUCKET}/"
    (
        df.write.mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(curated_path)
    )

    # ============================= AGGREGATION LAYER ==============================
    # We will compute:
    # ✅ total trips
    # ✅ avg fare amount
    # ✅ avg distance
    # ✅ total revenue (sum of total_amount)

    agg = (
        df.groupBy("ingestion_date")
          .agg(
              count("*").alias("total_trips"),
              avg("trip_distance").alias("avg_trip_distance"),
              avg("fare_amount").alias("avg_fare"),
              _sum("total_amount").alias("daily_revenue")
          )
          .orderBy("ingestion_date")
    )

    agg_path = f"s3a://{AGG_BUCKET}/snapshot_dt={time.strftime('%Y%m%d')}/"
    agg.write.mode("overwrite").parquet(agg_path)

    spark.stop()


if __name__ == "__main__":
    main()
