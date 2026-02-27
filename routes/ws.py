# ws.py - FIXED VERSION
import asyncio
import json
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
from services.query import get_latest_snapshot, get_option_latest

logger = logging.getLogger(__name__)

router = APIRouter()

client_subscriptions: dict = {}
connected_clients: set = set()
_last_snapshot: dict = None


async def broadcast_loop():
    """
    Background task that fetches data every 30s and broadcasts to all clients.
    Runs continuously even if errors occur.
    """
    global _last_snapshot
    logger.info("Broadcast loop started")

    while True:
        try:
            loop = asyncio.get_running_loop()
            logger.info("Fetching snapshot at %s", asyncio.get_event_loop().time())
            
            # FIX 1: Add timeout to prevent hanging
            try:
                snapshot = await asyncio.wait_for(
                    loop.run_in_executor(None, get_latest_snapshot),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.error("Snapshot fetch timeout after 10s - DB might be slow")
                await asyncio.sleep(30)
                continue

            _last_snapshot = snapshot

            total_options = sum(len(v) for v in snapshot.get('expiries', {}).values())
            expiries = list(snapshot.get('expiries', {}).keys())
            logger.info("Snapshot fetched — options: %d | expiries: %s", total_options, expiries)

            if total_options == 0:
                logger.warning("No data in snapshot — DB may be empty or market closed")
                await asyncio.sleep(30)
                continue

            # FIX 2: Batch all client sends to avoid blocking
            dead = set()
            send_tasks = []
            
            for client in list(connected_clients):  # Use list() to avoid iteration issues
                try:
                    # Send snapshot
                    snapshot_msg = json.dumps({"type": "snapshot", **snapshot})
                    send_tasks.append(
                        asyncio.wait_for(client.send_text(snapshot_msg), timeout=5.0)
                    )

                    # If client has subscription, prepare greeks data
                    sub = client_subscriptions.get(client)
                    if sub:
                        # FIX 3: Fetch metrics with timeout
                        try:
                            metrics = await asyncio.wait_for(
                                loop.run_in_executor(
                                    None,
                                    get_option_latest,
                                    sub['symbol'],
                                    sub['expiry']
                                ),
                                timeout=5.0
                            )
                            if metrics:
                                greeks_msg = json.dumps({"type": "greeks", **metrics})
                                send_tasks.append(
                                    asyncio.wait_for(client.send_text(greeks_msg), timeout=5.0)
                                )
                            else:
                                logger.warning("No metrics found for %s %s", sub['symbol'], sub['expiry'])
                        except asyncio.TimeoutError:
                            logger.error("Metrics fetch timeout for %s %s", sub['symbol'], sub['expiry'])

                except (ConnectionClosedOK, ConnectionClosedError) as e:
                    logger.info("Client connection closed during send: %s", e)
                    dead.add(client)
                except asyncio.TimeoutError:
                    logger.error("Send timeout for client - marking as dead")
                    dead.add(client)
                except Exception as e:
                    logger.error("Unexpected error preparing send for client: %s", e)
                    dead.add(client)

            # FIX 4: Execute all sends concurrently with error handling
            if send_tasks:
                results = await asyncio.gather(*send_tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error("Send task failed: %s", result)

            # Clean up dead clients
            if dead:
                connected_clients.difference_update(dead)
                for d in dead:
                    client_subscriptions.pop(d, None)
                logger.info("Removed %d dead clients", len(dead))

            logger.info("Broadcast complete — pushed to %d clients | options: %d", 
                       len(connected_clients), total_options)

        except Exception as e:
            # FIX 5: Catch-all to prevent loop from crashing
            logger.error("Broadcast loop critical error: %s", e, exc_info=True)

        await asyncio.sleep(30)


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for clients to receive real-time data.
    Handles subscriptions to specific options.
    """
    await websocket.accept()
    connected_clients.add(websocket)
    logger.info("Client connected | total: %d", len(connected_clients))

    # Send initial snapshot immediately
    if _last_snapshot:
        try:
            total = sum(len(v) for v in _last_snapshot.get('expiries', {}).values())
            await asyncio.wait_for(
                websocket.send_text(json.dumps({"type": "snapshot", **_last_snapshot})),
                timeout=5.0
            )
            logger.info("Sent initial snapshot — options: %d", total)
        except asyncio.TimeoutError:
            logger.error("Initial snapshot send timeout")
        except (ConnectionClosedOK, ConnectionClosedError) as e:
            logger.warning("Could not send initial snapshot: %s", e)
        except Exception as e:
            logger.error("Error sending initial snapshot: %s", e)
    else:
        logger.warning("No snapshot available yet for new client")

    try:
        while True:
            # FIX 8: Add timeout to receive to detect dead connections
            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=90.0)
            except asyncio.TimeoutError:
                # No message in 90s - send ping to check if alive
                try:
                    await websocket.send_text(json.dumps({"type": "ping"}))
                    continue
                except Exception:
                    logger.warning("Client not responding to ping")
                    break

            try:
                data = json.loads(msg)
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON from client: %s", e)
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON format"
                }))
                continue

            if "subscribe" in data:
                symbol = data.get("subscribe")
                expiry = data.get("expiry")

                if not symbol or not expiry:
                    logger.warning("Subscribe missing symbol or expiry")
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "Missing symbol or expiry"
                    }))
                    continue

                client_subscriptions[websocket] = {
                    "symbol": symbol,
                    "expiry": expiry
                }
                logger.info("Subscribed: %s %s", symbol, expiry)

                # FIX 6: Send immediate data after subscription
                try:
                    loop = asyncio.get_running_loop()
                    metrics = await asyncio.wait_for(
                        loop.run_in_executor(None, get_option_latest, symbol, expiry),
                        timeout=5.0
                    )
                    if metrics:
                        await websocket.send_text(json.dumps({"type": "greeks", **metrics}))
                        logger.info("Sent immediate greeks data for %s %s", symbol, expiry)
                    else:
                        logger.warning("No immediate data for %s %s", symbol, expiry)
                        await websocket.send_text(json.dumps({
                            "type": "info",
                            "message": "Subscribed - waiting for next update"
                        }))
                except asyncio.TimeoutError:
                    logger.error("Timeout fetching immediate data for %s %s", symbol, expiry)
                except Exception as e:
                    logger.error("Error fetching immediate data: %s", e)

            elif "unsubscribe" in data:
                client_subscriptions.pop(websocket, None)
                logger.info("Unsubscribed")
                await websocket.send_text(json.dumps({
                    "type": "info",
                    "message": "Unsubscribed successfully"
                }))

            elif "pong" in data:
                # Response to our ping
                logger.debug("Received pong from client")

    except WebSocketDisconnect:
        logger.info("Client disconnected gracefully")
    except (ConnectionClosedOK, ConnectionClosedError) as e:
        logger.info("Connection closed: %s", e)
    except Exception as e:
        # FIX 7: Catch-all for unexpected errors
        logger.error("WebSocket handler error: %s", e, exc_info=True)
    finally:
        connected_clients.discard(websocket)
        client_subscriptions.pop(websocket, None)
        logger.info("Client cleanup complete | remaining: %d", len(connected_clients))