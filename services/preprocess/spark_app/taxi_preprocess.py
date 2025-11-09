import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input CSV path, e.g. s3a://raw/ingest_dt=20250930/*.csv")
    parser.add_argument("--output", required=True, help="Output curated path, e.g. s3a://curated/taxi_data/")
    args = parser.parse_args()

    # Spark session with S3/MinIO config
    spark = (
        SparkSession.builder
        .appName("taxi-preprocess-simple")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    # Just read CSV
    df = spark.read.option("header", True).csv(args.input)

    # Debug: show first rows
    print("Schema:")
    df.printSchema()
    print("Sample rows:")
    df.show(5)

    # Write straight to parquet (no transformations)
    (df.write
       .mode("overwrite")
       .parquet(args.output.rstrip("/") + "/"))

    spark.stop()

if __name__ == "__main__":
    main()
