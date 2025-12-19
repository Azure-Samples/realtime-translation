"""
Main entry point for the Orchestrator real-time translation backend.

This FastAPI app provides endpoints for real-time audio and text translation using Azure OpenAI, Azure Communication Services, and Event Grid. It manages user sessions, WebSocket audio streaming, and ACS call automation events, and coordinates translation using the Command and Observer patterns.

Attributes:
    app (FastAPI): The FastAPI application instance.
    room_user_observer (RoomUserObserver): The main observer for user/room/call state.

"""

import os
import time
import struct
import asyncio
import psutil
from typing import Optional

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer
from fastapi_mcp import FastApiMCP
from starlette.middleware.cors import CORSMiddleware

from orchestrator import __app__, __author__, __version__, logger
from orchestrator.engine import Invoker
from orchestrator.engine.client import SpeechTranscriptionClient
from orchestrator.schemas import SuccessMessage
from orchestrator.background import lifespan
from orchestrator.utils import session_users
from orchestrator.analysis.microexp import analyze_frame
from orchestrator.storage.cosmos import CosmosCRUD


_HDR = struct.Struct("!IdH")


tags_metadata: list[dict] = [
    {
        "name": "Real-Time Translation",
        "description": "Endpoints for real-time audio and text translation using Azure OpenAI, Azure Communication Services (ACS), and Event Grid. Includes WebSocket streaming, session management, and translation orchestration.",
    },
    {
        "name": "Sessions",
        "description": "Session and user management for translation and communication rooms.",
    },
    {
        "name": "Health",
        "description": "Health check and monitoring endpoints for the orchestrator backend.",
    },
    {
        "name": "Admin",
        "description": "Administrative and utility endpoints for orchestration and diagnostics.",
    },
]

description: str = """
Azure Real-Time Translation Orchestrator

A robust FastAPI backend for real-time, low-latency audio and text translation using Azure OpenAI (gpt-4o-realtime), Azure Communication Services (ACS), and Event Grid. Features include:
- Real-time WebSocket endpoints for streaming audio and translation events
- Session and user management for multi-user rooms
- Integration with ACS for call automation and media streaming
- Event-driven architecture with clear separation of concerns
- Extensible, type-safe, and production-ready codebase

Follows Azure best practices for security, scalability, and maintainability.
"""

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
app = FastAPI(lifespan=lifespan, title=__app__, version=__version__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

mcp = FastApiMCP(app)


@app.get("/")
async def health_check(request: Request):
    """Health check endpoint with Azure-recommended metrics.

    Args:
        request (Request): The incoming HTTP request.

    Returns:
        SuccessMessage: Health and status metrics for the app.
    """
    # Fix: request.client may be None
    client_host = request.client.host if request.client else "unknown"
    logger.info("Health check endpoint called from %s", client_host)
    memory_usage = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    
    return SuccessMessage(
        title="App Running",
        message="App up and running. Check content for details",
        content={
            "timestamp": time.time(),
            "memory_mb": round(memory_usage, 2),
            "user_count": len(set(session_users.values())),
            "version": __version__
        }
    )

@app.websocket("/transcript-ws")
async def transcript_ws(websocket: WebSocket, session_id: Optional[str] = None):
    """Dedicated WebSocket endpoint for ACS real-time transcription frames.

    This endpoint expects ACS to connect (service-initiated) using the configured WEBSOCKET_URI.
    It does NOT open an Azure OpenAI realtime session; it only ingests transcription events and
    persists final results. The previous generic /ws endpoint remains available (if still used)
    for translation or other experimental flows.
    """
    await websocket.accept()
    headers = dict(websocket.headers)
    logger.info("[transcript-ws] Connection accepted client=%s headers=%s", websocket.client, {k: v for k, v in headers.items() if k.lower().startswith('x-ms')})
    call_connection_id = headers.get("x-ms-call-connection-id")
    trans_client = SpeechTranscriptionClient(session_id=session_id or call_connection_id)
    trans_client.ws = websocket
    # No Invoker: pure ingestion. Block until ACS disconnects or error occurs.
    try:
        await trans_client._from_acs_to_realtime()
    except Exception as e:
        logger.error(f"[transcript-ws] Error in transcription session: {e}")
    finally:
        if not websocket.application_state.name.lower().startswith("cLOS".lower()):  # defensive
            try:
                await websocket.close()
            except Exception:
                pass
    return

# Preserve legacy /ws behavior (could be repurposed for AOAI translation bridging)
@app.websocket("/ws")
async def ws(websocket: WebSocket, session_id: Optional[str] = None):
    await websocket.accept()
    logger.info("[/ws] Connection accepted (legacy/translation) client=%s headers=%s", websocket.client, dict(websocket.headers))
    call_connection_id = websocket.headers.get("x-ms-call-connection-id")
    trans_client = SpeechTranscriptionClient(session_id=session_id or call_connection_id)
    trans_client.ws = websocket
    try:
        keepalive = asyncio.create_task(trans_client.handle_realtime_messages())
        await trans_client._from_acs_to_realtime()
        keepalive.cancel()
    except Exception as e:
        logger.error(f"[/ws] Error handling session: {e}")
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
    return


@app.websocket("/micro/ws")
async def micro_ws(websocket: WebSocket, sessionId: str, participantId: Optional[str] = "local"):
    """Low-latency micro expression frame ingestion websocket.

    The client sends binary packets: [frameIndex:uint32][ts:float64][mimeLen:uint16][mime][payload].
    Currently only image/jpeg is expected.
    """
    await websocket.accept()
    logger.info("/micro/ws connected session=%s participant=%s", sessionId, participantId)
    # Chunked persistence setup
    CHUNK_SIZE = int(os.environ.get("MICROEXP_COSMOS_CHUNK", "10"))
    chunk_frames: list[dict] = []
    chunk_index = 0
    crud = CosmosCRUD('COSMOS_EXPRESSIONS_DATABASE')

    async def flush_chunk():
        nonlocal chunk_frames, chunk_index
        if not chunk_frames:
            return
        doc = {
            "id": f"{sessionId}:chunk:{chunk_index}",
            "sessionId": sessionId,
            "participantId": participantId,
            "type": "microexp_chunk_v1",
            "chunkIndex": chunk_index,
            "startFrame": chunk_frames[0]["frameIndex"],
            "endFrame": chunk_frames[-1]["frameIndex"],
            "frameCount": len(chunk_frames),
            "microEventCount": sum(1 for f in chunk_frames if f.get("microEvent")),
            "frames": chunk_frames,
            "tsLast": chunk_frames[-1]["ts"],
        }
        try:
            await crud.create_item(doc)
        except Exception as e:
            logger.error(f"[micro/ws] Failed to persist microexp chunk {chunk_index} for {sessionId}: {e}")
        chunk_index += 1
        chunk_frames = []

    try:
        while True:
            data: bytes = await websocket.receive_bytes()
            if len(data) < _HDR.size:
                continue
            frameIndex, tsSec, mimeLen = _HDR.unpack_from(data, 0)
            off = _HDR.size
            if len(data) < off + mimeLen:
                continue
            mime = data[off:off+mimeLen].decode('utf-8', 'ignore')
            off += mimeLen
            frame_bytes = data[off:]
            if mime != "image/jpeg":
                continue
            frame_res = analyze_frame(sessionId, int(frameIndex), float(tsSec), frame_bytes)
            chunk_frames.append(frame_res)
            if len(chunk_frames) >= CHUNK_SIZE:
                await flush_chunk()
    except Exception as e:
        logger.debug("/micro/ws closed session=%s reason=%s", sessionId, getattr(e, 'message', repr(e)))
    finally:
        try:
            await flush_chunk()
            # Optional session summary document
            from orchestrator.analysis.microexp import summarize_session
            summary = summarize_session(sessionId)
            summary_doc = {
                "id": f"{sessionId}:summary",
                "sessionId": sessionId,
                "participantId": participantId,
                "type": "microexp_summary_v1",
                **summary,
            }
            try:
                await crud.create_item(summary_doc)
            except Exception as e:
                logger.error(f"[micro/ws] Failed to persist summary for {sessionId}: {e}")
        except Exception:
            pass
        await websocket.close()


@app.post("/incoming-call")
async def incoming_call_handler(request: Request):
    """Handle incoming ACS call events from Event Grid.

    Enhanced: Supports optional feature flags in JSON body for selective enabling of
    transcription and/or media streaming. Expected payload (array of events) OR an
    object containing { "events": [...], "features": {"transcription": true, "media": false } }.
    Default behavior: transcription enabled, media disabled.
    """

    raw = await request.json()
    # Support both raw array (original Event Grid) and wrapped object with features
    if isinstance(raw, dict) and "events" in raw:
        events = raw.get("events", [])
        features = raw.get("features", {})
    else:
        events = raw if isinstance(raw, list) else []
        features = {}

    enable_transcription = bool(features.get("transcription", True))
    enable_media = bool(features.get("media", False))
    if enable_transcription and enable_media:
        # We'll warn here; observer will also log and prefer transcription.
        logger.warning("/incoming-call both transcription and media requested; transcription will take precedence.")

    room_user_observer = app.state.room_user_observer
    for event_dict in events:
        result = await room_user_observer.handle_incoming_call(
            event_dict,
            enable_transcription=enable_transcription,
            enable_media=enable_media
        )
        if result and isinstance(result, dict) and "validationResponse" in result:
            return JSONResponse(content={"validationResponse": result["validationResponse"]})
    return SuccessMessage(title="Incoming call handler", message="Incoming call event(s) processed successfully", content={"transcription": enable_transcription, "media": enable_media})


@app.post("/callbacks/{contextId}")
async def callbacks(request: Request):
    """Handle ACS callback events (CallConnected, MediaStreamingStarted, etc).

    Args:
        request (Request): The incoming HTTP request with callback events.

    Returns:
        SuccessMessage: Result of callback event handling.
    """
    data = await request.json()
    room_user_observer = request.app.state.room_user_observer
    for event in data:
        await room_user_observer.handle_callback_event(event)
    return SuccessMessage(title="Incoming call handler", message="Incoming call event processed successfully")


@app.get("/admin/calls")
async def admin_calls(request: Request, failed: Optional[bool] = None):
    """List active call connections with transcription / media status.

    Query params:
        failed (bool, optional): If provided, filter only calls whose transcription_failed matches this value.
    """
    observer = request.app.state.room_user_observer
    results = []
    for ci in observer._call_connections.values():  # type: ignore[attr-defined]
        if failed is not None and ci.transcription_failed != failed:  # type: ignore[attr-defined]
            continue
        results.append({
            "call_connection_id": ci.call_connection_id,
            "room_id": ci.room_id,
            "websocket_url": ci.websocket_url,
            "transcription_failed": ci.transcription_failed,  # type: ignore[attr-defined]
            "transcription_error_code": ci.transcription_error_code,  # type: ignore[attr-defined]
            "transcription_error_subcode": ci.transcription_error_subcode,  # type: ignore[attr-defined]
            "transcription_error_message": ci.transcription_error_message,  # type: ignore[attr-defined]
            "transcription_status_details": ci.transcription_status_details,  # type: ignore[attr-defined]
            "transcription_last_failure_time": ci.transcription_last_failure_time,  # type: ignore[attr-defined]
            "metadata_present": ci.transcription_metadata is not None,  # type: ignore[attr-defined]
            "transcription_start_requested": getattr(ci, 'transcription_start_requested', False),
            "transcription_started": getattr(ci, 'transcription_started', False),
            "transcription_start_attempts": getattr(ci, 'transcription_start_attempts', 0),
            "transcription_last_start_error": getattr(ci, 'transcription_last_start_error', None),
            "transcription_last_start_time": getattr(ci, 'transcription_last_start_time', None)
        })
    return SuccessMessage(
        title="Active Calls",
        message=f"Returned {len(results)} call(s).",
        content={"count": len(results), "calls": results}
    )


mcp.mount()
