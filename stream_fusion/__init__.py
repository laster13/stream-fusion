"""stream_fusion package."""

import asyncio
from fastapi import FastAPI
from .services.postgresql.db_init import init_db

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    await init_db()
