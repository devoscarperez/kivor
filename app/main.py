from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers.auth import router as auth_router
from routers.customers_express import router as customers_express_router
from routers.dashboard import router as dashboard_router

app = FastAPI(title="KIVOR Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(customers_express_router)
app.include_router(dashboard_router)

@app.get("/")
def root():
    return {"status": "ok", "service": "kivor-backend"}
