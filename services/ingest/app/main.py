
import os, sys, time, pathlib, uuid
import boto3
from botocore.client import Config
from datetime import datetime

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")

DATA_SRC = "/data_source"

def client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name=os.getenv("MINIO_REGION","us-east-1"),
    )

def main():
    s3 = client()
    count = 0
    ts = datetime.utcnow().strftime("%Y%m%d")
    base_prefix = f"ingest_dt={ts}/"
    for p in pathlib.Path(DATA_SRC).glob("**/*"):
        if p.is_file():
            key = base_prefix + p.name
            print(f"Uploading {p} -> s3://{BUCKET}/{key}")
            s3.upload_file(str(p), BUCKET, key)
            count += 1
    print(f"Uploaded {count} files.")
    if count == 0:
        print("No files found in ./data_source. Please place your time-stamped files there.", file=sys.stderr)

if __name__ == "__main__":
    main()
