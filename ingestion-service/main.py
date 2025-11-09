# ingestion-service: minimal FastAPI app that writes file to MinIO (placeholder)
from fastapi import FastAPI
import os,time

app = FastAPI()

@app.get('/health')
def health():
    return {'status':'ok','service':'ingestion-service'}

@app.get('/ingest_sample')
def ingest_sample():
    # Placeholder – implement real ingestion logic here
    timestamp = int(time.time())
    filename = f"sample_{timestamp}.txt"
    with open(filename,'w') as f:
        f.write('placeholder data')
    return {'filename': filename}
