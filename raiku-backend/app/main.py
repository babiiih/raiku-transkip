from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import httpx
import asyncio
import json
import websockets

app = FastAPI()

# Disable CORS. Do not remove this for full-stack development.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

LIVENESS_BASE = "https://liveness.cc"
GUILD_ID = "1337420798754947173"
WS_URL = "wss://liveness.cc/ws/transcription"


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/api/sessions")
async def get_sessions():
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{LIVENESS_BASE}/api/transcription/{GUILD_ID}/sessions?limit=20",
            timeout=15.0,
        )
        return resp.json()


@app.get("/api/session/{session_id}/logs")
async def get_session_logs(session_id: int, limit: int = 100):
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{LIVENESS_BASE}/api/transcription/{GUILD_ID}/sessions/{session_id}/logs",
            timeout=15.0,
        )
        return resp.json()


@app.get("/api/session/{session_id}/export")
async def export_session(session_id: int):
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{LIVENESS_BASE}/api/transcription/session/{session_id}/export",
            timeout=30.0,
        )
        return resp.json()


@app.websocket("/ws/transcription")
async def websocket_proxy(websocket: WebSocket):
    await websocket.accept()

    try:
        async with websockets.connect(
            WS_URL,
            additional_headers={"Origin": LIVENESS_BASE},
        ) as ws_upstream:
            subscribe_msg = json.dumps({
                "type": "subscribe",
                "guildId": GUILD_ID,
            })
            await ws_upstream.send(subscribe_msg)

            async def forward_upstream():
                try:
                    async for message in ws_upstream:
                        await websocket.send_text(message)
                except Exception:
                    pass

            async def forward_client():
                try:
                    while True:
                        data = await websocket.receive_text()
                        await ws_upstream.send(data)
                except WebSocketDisconnect:
                    pass
                except Exception:
                    pass

            await asyncio.gather(forward_upstream(), forward_client())

    except Exception as e:
        try:
            await websocket.send_text(json.dumps({"error": str(e)}))
        except Exception:
            pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
