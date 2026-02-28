from contextlib import asynccontextmanager
import asyncio
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.ws import router as ws_router, broadcast_loop
from routes.history import router as history_router
from routes.snapshot import router as snapshot_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    asyncio.create_task(broadcast_loop())
    yield
    # Shutdown (add cleanup here if needed)


app = FastAPI(title="Risk Monitor API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ws_router)
app.include_router(history_router)
app.include_router(snapshot_router)


@app.get("/health")
def health():
    """
    Returns the health status of the API.

    Returns:
        dict: A JSON object with a single key "status" set to "ok".
    """
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)