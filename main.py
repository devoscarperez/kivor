from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import auth
from routers import customers_express
from routers import dashboard

app = FastAPI(title="KIVOR Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(customers_express.router)
app.include_router(dashboard.router)


@app.get("/")
def root():
    return {"status": "ok", "service": "kivor-backend"}
