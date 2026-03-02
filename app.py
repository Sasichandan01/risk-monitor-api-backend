from contextlib import asynccontextmanager
import asyncio
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware # Add this import
from routes.ws import router as ws_router, broadcast_loop
from routes.history import router as history_router
from routes.snapshot import router as snapshot_router

# --- ADD THIS MIDDLEWARE ---
class WebSocketOriginMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # If it's a WebSocket upgrade request, remove the Origin header
        # This prevents Starlette's internal 403 check for proxies
        if request.headers.get("upgrade") == "websocket":
            scope = request.scope
            new_headers = []
            for key, value in scope["headers"]:
                if key.lower() != b"origin":
                    new_headers.append((key, value))
            scope["headers"] = new_headers
        return await call_next(request)
# ---------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(broadcast_loop())
    yield

app = FastAPI(title="Risk Monitor API", lifespan=lifespan)

# Add the new middleware FIRST
app.add_middleware(WebSocketOriginMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True, # Added for better compatibility
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ws_router)
app.include_router(history_router)
app.include_router(snapshot_router)

@app.get("/api/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    # Ensure proxy_headers is True if running via python app.py
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False, proxy_headers=True)