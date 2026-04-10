"""Tests for the admin dashboard API routes."""
import json as _json
import os
import tempfile
import pytest

# Set env vars BEFORE any dashboard imports so db.py picks them up
_tmpdir = tempfile.mkdtemp()
os.environ.setdefault("ADMIN_PASSWORD", "testpass123")
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ["DB_PATH"] = os.path.join(_tmpdir, "test.db")
os.environ["DOWNLOADS_PATH"] = _tmpdir

from fastapi.testclient import TestClient
from main import create_app

app = create_app()
client = TestClient(app, raise_server_exceptions=True)


def _delete(path: str, body: dict, cookies: dict | None = None):
    """Helper: send DELETE with JSON body using client.request()."""
    kwargs = {
        "content": _json.dumps(body).encode(),
        "headers": {"content-type": "application/json"},
    }
    if cookies:
        kwargs["cookies"] = cookies
    return client.request("DELETE", path, **kwargs)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _login() -> dict:
    """POST /login with correct creds and return the response cookies."""
    resp = client.post(
        "/login",
        data={"username": "admin", "password": "testpass123"},
        follow_redirects=False,
    )
    assert resp.status_code == 303, f"Login failed: {resp.status_code} {resp.text}"
    return resp.cookies


# ---------------------------------------------------------------------------
# Event ingestion (no auth)
# ---------------------------------------------------------------------------

def test_event_download_start():
    resp = client.post("/api/events", json={
        "type": "download_start",
        "data": {
            "job_id": "job-start-1",
            "user_id": 42,
            "username": "alice",
            "chat_id": 99,
            "url": "https://example.com/video.mp4",
            "platform": "youtube",
        },
    })
    assert resp.status_code == 204


def test_event_download_progress():
    # Start one first
    client.post("/api/events", json={
        "type": "download_start",
        "data": {
            "job_id": "job-progress-1",
            "user_id": 1,
            "username": "bob",
            "chat_id": 1,
            "url": "https://example.com/v.mp4",
        },
    })
    resp = client.post("/api/events", json={
        "type": "download_progress",
        "data": {
            "job_id": "job-progress-1",
            "progress": 50.0,
            "speed": "1 MB/s",
            "eta": "5s",
        },
    })
    assert resp.status_code == 204


def test_event_download_done():
    client.post("/api/events", json={
        "type": "download_start",
        "data": {
            "job_id": "job-done-1",
            "user_id": 2,
            "username": "carol",
            "chat_id": 2,
            "url": "https://example.com/done.mp4",
            "platform": "tiktok",
        },
    })
    resp = client.post("/api/events", json={
        "type": "download_done",
        "data": {
            "job_id": "job-done-1",
            "title": "My Video",
            "format": "mp4",
            "quality": "1080p",
            "file_size_bytes": 1024000,
            "download_duration_sec": 3.5,
        },
    })
    assert resp.status_code == 204


def test_event_download_error():
    client.post("/api/events", json={
        "type": "download_start",
        "data": {
            "job_id": "job-err-1",
            "user_id": 3,
            "username": "dave",
            "chat_id": 3,
            "url": "https://example.com/err.mp4",
        },
    })
    resp = client.post("/api/events", json={
        "type": "download_error",
        "data": {
            "job_id": "job-err-1",
            "error_message": "HTTP 403 forbidden",
        },
    })
    assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Auth: dashboard-stats
# ---------------------------------------------------------------------------

def test_dashboard_stats_without_auth_returns_401():
    resp = client.get("/api/dashboard-stats")
    assert resp.status_code == 401


def test_dashboard_stats_with_auth_returns_200():
    cookies = _login()
    resp = client.get("/api/dashboard-stats", cookies=cookies)
    assert resp.status_code == 200
    body = resp.json()
    assert "stats" in body
    stats = body["stats"]
    assert "downloads_today" in stats
    assert "active_users_24h" in stats
    assert "error_rate" in stats


# ---------------------------------------------------------------------------
# Chart data
# ---------------------------------------------------------------------------

def test_chart_data_without_auth_returns_401():
    fresh = TestClient(app, raise_server_exceptions=True)
    resp = fresh.get("/api/chart-data?range=1D")
    assert resp.status_code == 401


def test_chart_data_with_auth():
    cookies = _login()
    for range_key in ("1D", "7D", "1M", "1Y"):
        resp = client.get(f"/api/chart-data?range={range_key}", cookies=cookies)
        assert resp.status_code == 200, f"Failed for range={range_key}"
        body = resp.json()
        assert "labels" in body
        assert "values" in body


# ---------------------------------------------------------------------------
# Active downloads
# ---------------------------------------------------------------------------

def test_active_downloads_without_auth_returns_401():
    fresh = TestClient(app, raise_server_exceptions=True)
    resp = fresh.get("/api/active-downloads")
    assert resp.status_code == 401


def test_active_downloads_with_auth():
    # Start a job to make sure at least one is active
    client.post("/api/events", json={
        "type": "download_start",
        "data": {
            "job_id": "job-active-1",
            "user_id": 10,
            "username": "eve",
            "chat_id": 10,
            "url": "https://example.com/active.mp4",
            "platform": "instagram",
        },
    })
    cookies = _login()
    resp = client.get("/api/active-downloads", cookies=cookies)
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    job_ids = [d["job_id"] for d in body]
    assert "job-active-1" in job_ids


# ---------------------------------------------------------------------------
# Task 5: Delete files
# ---------------------------------------------------------------------------

def test_delete_files():
    """Create a temp file, delete it via API, verify it's gone."""
    import pathlib

    downloads_path = pathlib.Path(_tmpdir)
    test_file = downloads_path / "test_delete_me.mp4"
    test_file.write_bytes(b"fake video content")
    assert test_file.exists()

    cookies = _login()
    resp = _delete("/api/files", {"paths": ["test_delete_me.mp4"]}, cookies=cookies)
    assert resp.status_code == 200
    body = resp.json()
    assert "test_delete_me.mp4" in body["deleted"]
    assert not test_file.exists()


def test_delete_files_without_auth_returns_401():
    fresh = TestClient(app, raise_server_exceptions=True)
    resp = fresh.request(
        "DELETE",
        "/api/files",
        content=_json.dumps({"paths": ["something.mp4"]}).encode(),
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 401


def test_purge_requires_confirm():
    """Sending empty/missing confirm should return 400."""
    cookies = _login()
    resp = _delete("/api/files/all", {}, cookies=cookies)
    assert resp.status_code == 400


def test_purge_with_confirm():
    """Create a temp file, purge all, verify it's gone."""
    import pathlib

    downloads_path = pathlib.Path(_tmpdir)
    test_file = downloads_path / "purge_me.mp4"
    test_file.write_bytes(b"content to be purged")
    assert test_file.exists()

    cookies = _login()
    resp = _delete("/api/files/all", {"confirm": "PURGE"}, cookies=cookies)
    assert resp.status_code == 200
    body = resp.json()
    assert "deleted_count" in body
    assert not test_file.exists()
