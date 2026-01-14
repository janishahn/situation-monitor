from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from fastapi import APIRouter, Request
from starlette.responses import StreamingResponse

from realtime.bus import EventBus


router = APIRouter()


@router.get("/sse")
async def sse(request: Request) -> StreamingResponse:
    bus: EventBus = request.app.state.bus
    queue = await bus.subscribe()

    async def event_stream():
        try:
            yield "event: heartbeat\ndata: {}\n\n"
            while True:
                if await request.is_disconnected():
                    return
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    ts = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
                    yield f"event: heartbeat\ndata: {json.dumps({'ts': ts})}\n\n"
                    continue

                data = json.dumps(event.data, separators=(",", ":"), ensure_ascii=False)
                yield f"event: {event.type}\ndata: {data}\n\n"
        finally:
            await bus.unsubscribe(queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )
