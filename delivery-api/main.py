# delivery-api: minimal FastAPI app to serve aggregated results (placeholder)
from fastapi import FastAPI
app = FastAPI()

@app.get('/health')
def health():
    return {'status':'ok','service':'delivery-api'}

@app.get('/result')
def result():
    return {'message':'aggregated result placeholder'}
