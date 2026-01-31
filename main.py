from fastapi import FastAPI

app = FastAPI(title="KIVOR Backend")

@app.get("/")
def root():
    return {"status": "ok", "service": "kivor-backend"}

@app.get("/health")
def health():
    return {"healthy": True}
