"""Page routes for the reclip_bot admin dashboard."""
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

import db
from auth import verify_credentials, create_session_cookie, get_current_user, COOKIE_NAME

router = APIRouter()

_templates_dir = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=str(_templates_dir))


def _require_auth(request: Request) -> str:
    user = get_current_user(request)
    if not user:
        return None
    return user


# ---------------------------------------------------------------------------
# Login / Logout
# ---------------------------------------------------------------------------

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login")
async def login_submit(request: Request) -> HTMLResponse:
    form = await request.form()
    username = form.get("username", "")
    password = form.get("password", "")

    if verify_credentials(username, password):
        response = RedirectResponse(url="/", status_code=303)
        create_session_cookie(response, username)
        return response

    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": "Invalid credentials"},
        status_code=401,
    )


@router.get("/logout")
async def logout() -> RedirectResponse:
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(COOKIE_NAME)
    return response


# ---------------------------------------------------------------------------
# Protected pages
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request) -> HTMLResponse:
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": user})


@router.get("/history", response_class=HTMLResponse)
async def history_page(
    request: Request,
    page: int = 1,
    platform: str = None,
    status: str = None,
    user_filter: str = None,
    date_from: str = None,
    date_to: str = None,
) -> HTMLResponse:
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    data = await db.get_downloads_page(
        page=page,
        platform=platform,
        status=status,
        user=user_filter,
        date_from=date_from,
        date_to=date_to,
    )
    return templates.TemplateResponse(
        "history.html",
        {
            "request": request,
            "user": user,
            "data": data,
            "platform": platform,
            "status": status,
            "user_filter": user_filter,
            "date_from": date_from,
            "date_to": date_to,
        },
    )


@router.get("/errors", response_class=HTMLResponse)
async def errors_page(
    request: Request,
    date_from: str = None,
    date_to: str = None,
) -> HTMLResponse:
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    errors = await db.get_error_downloads(date_from=date_from, date_to=date_to)
    return templates.TemplateResponse(
        "errors.html",
        {"request": request, "user": user, "errors": errors},
    )


@router.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request) -> HTMLResponse:
    import os
    import shutil

    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    downloads_path = Path(os.environ.get("DOWNLOADS_PATH", "/downloads"))
    files = []
    if downloads_path.exists():
        for f in sorted(downloads_path.iterdir()):
            try:
                stat = f.stat()
                files.append({
                    "name": f.name,
                    "size": stat.st_size,
                    "is_dir": f.is_dir(),
                })
            except OSError:
                pass

    disk_total, disk_used, disk_free = shutil.disk_usage(str(downloads_path) if downloads_path.exists() else "/")

    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "user": user,
            "files": files,
            "disk_total": disk_total,
            "disk_used": disk_used,
            "disk_free": disk_free,
        },
    )
