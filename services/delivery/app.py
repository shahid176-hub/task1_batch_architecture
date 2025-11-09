
import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import boto3
from botocore.client import Config
from datetime import timedelta, datetime

app = FastAPI(title="Aggregates Delivery API", version="0.1.0")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
BUCKET = os.getenv("MINIO_BUCKET_AGG", "aggregated")
REGION = os.getenv("MINIO_REGION", "us-east-1")

def s3():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name=REGION,
    )

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}

@app.get("/aggregates")
def list_aggregates(prefix: str = ""):
    s = s3()
    resp = s.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    contents = resp.get("Contents", [])
    files = [c["Key"] for c in contents if c["Key"].endswith(".parquet") or c["Key"].endswith("/")]
    return {"bucket": BUCKET, "files": files}

@app.get("/presign")
def presign(key: str):
    try:
        url = s3().generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": BUCKET, "Key": key},
            ExpiresIn=3600,
        )
        return {"url": url, "expires_in": 3600}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
