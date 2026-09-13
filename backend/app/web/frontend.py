"""Serve the React application alongside the API on Replit."""
from pathlib import Path

from starlette.exceptions import HTTPException
from starlette.staticfiles import StaticFiles


class SPAStaticFiles(StaticFiles):
    async def get_response(self, path, scope):
        path = path.replace("\\", "/")
        if path == "api" or path.startswith(("api/", "ws/")):
            raise HTTPException(404)
        try:
            return await super().get_response(path, scope)
        except HTTPException as exc:
            if exc.status_code != 404 or Path(path).suffix or path.startswith("static/"):
                raise
            return await super().get_response("index.html", scope)


def mount_frontend(app, build_directory: Path) -> bool:
    """Call after registering API and WebSocket routes so they take priority."""
    if not (build_directory / "index.html").is_file():
        return False
    app.mount("/", SPAStaticFiles(directory=str(build_directory), html=True), name="frontend")
    return True
