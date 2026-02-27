from fastapi import APIRouter
from services.query import get_latest_snapshot

router = APIRouter()


@router.get("/api/snapshot")
async def snapshot():
    """
    REST fallback for snapshot.
    Frontend calls this once on load to get initial state
    before WebSocket connects.
    """
    data = get_latest_snapshot()
    return data