from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg, count, date_format
import os, time

RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")
CUR_BUCKET = os.getenv("MINIO_BUCKET_CURATED", "curated")
AGG_BUCKET = os.getenv("MINIO_BUCKET_AGG", "aggregated")
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

def build_spark():
    return (SparkSession.builder
        .appName("nyc-taxi-batch-processing")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate())

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_path = f"s3a://{RAW_BUCKET}/"
    df = spark.read.csv(raw_path, header=True, inferSchema=True)

    # NYC Taxi dataset specific columns
    df = (
        df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
          .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    )

    curated_path = f"s3a://{CUR_BUCKET}/"
    df.write.mode("overwrite").parquet(curated_path)

    agg = (df.groupBy(date_format(col("pickup_datetime"), "yyyy-MM-dd").alias("trip_date"))
             .agg(
                 avg("trip_distance").alias("avg_trip_distance"),
                 count("*").alias("trip_count")
             )
          )

    agg_path = f"s3a://{AGG_BUCKET}/snapshot_dt={time.strftime('%Y%m%d')}/"
    agg.write.mode("overwrite").parquet(agg_path)

    print(" Spark ETL Completed.")
    spark.stop()

if __name__ == "__main__":
    main()
