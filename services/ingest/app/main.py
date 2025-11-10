import os
import boto3
import time

RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

DATA_SOURCE_FOLDER = "/data_source"

def upload_to_minio(client, file_path, object_name):
    print(f"➡ Uploading {file_path} → {RAW_BUCKET}/{object_name}")
    client.upload_file(
        Filename=file_path,
        Bucket=RAW_BUCKET,
        Key=object_name
    )

def main():
    print(" Starting ingest service...")

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
    )

    for file in os.listdir(DATA_SOURCE_FOLDER):
        if file.endswith(".csv"):
            file_path = os.path.join(DATA_SOURCE_FOLDER, file)
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            object_name = f"{timestamp}_{file}"
            upload_to_minio(s3_client, file_path, object_name)

    print(" Ingestion completed.")

if __name__ == "__main__":
    main()
