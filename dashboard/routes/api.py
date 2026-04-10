"""API routes for the reclip_bot admin dashboard."""
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import db
from auth import get_current_user

router = APIRouter()

# In-memory active downloads dict: job_id -> dict
_active_downloads: Dict[str, Dict[str, Any]] = {}


def _downloads_path() -> Path:
    return Path(os.environ.get("DOWNLOADS_PATH", "/downloads"))


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------

def require_auth(request: Request) -> str:
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


# ---------------------------------------------------------------------------
# Event ingestion (no auth — internal Docker network only)
# ---------------------------------------------------------------------------

class DownloadStartEvent(BaseModel):
    job_id: str
    user_id: Optional[int] = None
    username: Optional[str] = None
    chat_id: Optional[int] = None
    url: str
    platform: Optional[str] = None


class DownloadProgressEvent(BaseModel):
    job_id: str
    progress: Optional[float] = None
    speed: Optional[str] = None
    eta: Optional[str] = None


class DownloadDoneEvent(BaseModel):
    job_id: str
    title: Optional[str] = None
    format: Optional[str] = None
    quality: Optional[str] = None
    file_size_bytes: Optional[int] = None
    download_duration_sec: Optional[float] = None


class DownloadErrorEvent(BaseModel):
    job_id: str
    error_message: str


class EventEnvelope(BaseModel):
    type: str
    data: Dict[str, Any]


@router.post("/api/events", status_code=204)
async def ingest_event(envelope: EventEnvelope) -> None:
    """Accept download lifecycle events. No auth — internal network only."""
    event_type = envelope.type
    data = envelope.data

    if event_type == "download_start":
        ev = DownloadStartEvent(**data)
        await db.insert_download_start(
            job_id=ev.job_id,
            user_id=ev.user_id,
            username=ev.username,
            chat_id=ev.chat_id,
            url=ev.url,
            platform=ev.platform,
        )
        _active_downloads[ev.job_id] = {
            "job_id": ev.job_id,
            "user_id": ev.user_id,
            "username": ev.username,
            "url": ev.url,
            "platform": ev.platform,
            "progress": None,
            "speed": None,
            "eta": None,
        }

    elif event_type == "download_progress":
        ev = DownloadProgressEvent(**data)
        if ev.job_id in _active_downloads:
            _active_downloads[ev.job_id].update({
                "progress": ev.progress,
                "speed": ev.speed,
                "eta": ev.eta,
            })

    elif event_type == "download_done":
        ev = DownloadDoneEvent(**data)
        await db.update_download_done(
            job_id=ev.job_id,
            title=ev.title,
            format=ev.format,
            quality=ev.quality,
            file_size_bytes=ev.file_size_bytes,
            download_duration_sec=ev.download_duration_sec,
        )
        _active_downloads.pop(ev.job_id, None)

    elif event_type == "download_error":
        ev = DownloadErrorEvent(**data)
        await db.update_download_error(
            job_id=ev.job_id,
            error_message=ev.error_message,
        )
        _active_downloads.pop(ev.job_id, None)

    else:
        raise HTTPException(status_code=400, detail=f"Unknown event type: {event_type}")


# ---------------------------------------------------------------------------
# Dashboard stats (auth required)
# ---------------------------------------------------------------------------

@router.get("/api/dashboard-stats")
async def dashboard_stats(user: str = Depends(require_auth)) -> Dict[str, Any]:
    stats = await db.get_dashboard_stats()
    disk = await db.get_latest_disk_snapshot()
    return {"stats": stats, "disk": disk}


@router.get("/api/chart-data")
async def chart_data(
    range: str = "1D",
    user: str = Depends(require_auth),
) -> Dict[str, Any]:
    valid_ranges = {"1D", "7D", "1M", "1Y"}
    if range not in valid_ranges:
        raise HTTPException(status_code=400, detail=f"Invalid range. Must be one of {valid_ranges}")
    return await db.get_chart_data(range)


@router.get("/api/active-downloads")
async def active_downloads(user: str = Depends(require_auth)) -> List[Dict[str, Any]]:
    return list(_active_downloads.values())


# ---------------------------------------------------------------------------
# Admin file operations (auth required) — Task 5
# ---------------------------------------------------------------------------

class DeleteFilesBody(BaseModel):
    paths: List[str]


class PurgeBody(BaseModel):
    confirm: Optional[str] = None


@router.delete("/api/files")
async def delete_files(
    body: DeleteFilesBody,
    user: str = Depends(require_auth),
) -> Dict[str, Any]:
    """Delete selected files. Uses filename only to prevent path traversal."""
    downloads_path = _downloads_path()
    deleted = []
    errors = []
    for p in body.paths:
        # Prevent path traversal: use only the filename component
        safe_path = downloads_path / Path(p).name
        try:
            if safe_path.exists():
                safe_path.unlink()
                deleted.append(str(safe_path.name))
            else:
                errors.append({"file": p, "error": "not found"})
        except Exception as exc:
            errors.append({"file": p, "error": str(exc)})
    return {"deleted": deleted, "errors": errors}


@router.delete("/api/files/all")
async def purge_all_files(
    body: PurgeBody,
    user: str = Depends(require_auth),
) -> Dict[str, Any]:
    """Purge all files in DOWNLOADS_PATH. Requires confirm='PURGE' in body."""
    if body.confirm != "PURGE":
        raise HTTPException(status_code=400, detail='Body must contain {"confirm": "PURGE"}')
    downloads_path = _downloads_path()
    deleted_count = 0
    if downloads_path.exists():
        for item in downloads_path.iterdir():
            try:
                if item.is_file():
                    item.unlink()
                    deleted_count += 1
                elif item.is_dir():
                    shutil.rmtree(item)
                    deleted_count += 1
            except Exception:
                pass
    return {"deleted_count": deleted_count}
