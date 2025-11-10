from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("batch_transform_job") \
    .getOrCreate()

# Read RAW data from MinIO (object storage)
df = spark.read.csv(
    "s3a://raw/*.csv",
    header=True,
    inferSchema=True
)

# Transformation example: rename/clean
clean_df = df.withColumnRenamed("id", "user_id")

# Store PROCESSed output back to MinIO
clean_df.write.mode("overwrite").parquet("s3a://processed/")

spark.stop()
