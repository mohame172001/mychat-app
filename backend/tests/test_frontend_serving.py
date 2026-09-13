import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient

from app.web.frontend import mount_frontend


class FrontendServingTests(unittest.TestCase):
    def test_spa_routes_api_errors_and_websocket_are_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            (build / "index.html").write_text("<html>mychat</html>", encoding="utf-8")
            app = FastAPI()

            @app.get("/api/health")
            def health():
                return {"ok": True}

            @app.websocket("/ws/test")
            async def socket(ws: WebSocket):
                await ws.accept()
                await ws.send_text("pong")
                await ws.close()

            self.assertTrue(mount_frontend(app, build))
            with TestClient(app) as client:
                for path in ("/", "/app", "/app/automations"):
                    self.assertEqual(client.get(path).text, "<html>mychat</html>")
                self.assertEqual(client.get("/api/health").json(), {"ok": True})
                for path in ("/api/missing", "/api", "/static/missing.js", "/missing.png"):
                    self.assertEqual(client.get(path).status_code, 404)
                with client.websocket_connect("/ws/test") as socket_client:
                    self.assertEqual(socket_client.receive_text(), "pong")

    def test_missing_bundle_does_not_mount_empty_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertFalse(mount_frontend(FastAPI(), Path(directory)))
