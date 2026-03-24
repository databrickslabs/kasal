"""
Unit tests for entrypoint.py static asset serving.

Tests that all Kasal icon variants are served correctly.
"""
import os
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import FastAPI
from fastapi.testclient import TestClient


class TestKasalIconRoutes:
    """Test that kasal icon static assets are served."""

    def _create_app_with_static_dir(self, files_present):
        """Helper: create a FastAPI app and register static file routes
        exactly as entrypoint.py does, but with a mocked filesystem."""
        from fastapi.responses import FileResponse

        app = FastAPI()
        frontend_static_dir = "/fake/frontend_static"

        def fake_exists(path):
            basename = os.path.basename(path)
            return basename in files_present or path == frontend_static_dir

        with patch("os.path.exists", side_effect=fake_exists):
            # Replicate the entrypoint pattern for icon routes
            kasal_icon_16 = os.path.join(frontend_static_dir, "kasal-icon-16.png")
            if fake_exists(kasal_icon_16):
                @app.get("/kasal-icon-16.png")
                async def serve_kasal_icon_16():
                    return FileResponse(kasal_icon_16)

            kasal_icon_24 = os.path.join(frontend_static_dir, "kasal-icon-24.png")
            if fake_exists(kasal_icon_24):
                @app.get("/kasal-icon-24.png")
                async def serve_kasal_icon_24():
                    return FileResponse(kasal_icon_24)

        return app

    def test_kasal_icon_16_route_registered(self):
        """kasal-icon-16.png route is registered when file exists."""
        app = self._create_app_with_static_dir({"kasal-icon-16.png"})
        routes = [r.path for r in app.routes]
        assert "/kasal-icon-16.png" in routes

    def test_kasal_icon_24_route_registered(self):
        """kasal-icon-24.png route is registered when file exists."""
        app = self._create_app_with_static_dir({"kasal-icon-24.png"})
        routes = [r.path for r in app.routes]
        assert "/kasal-icon-24.png" in routes

    def test_both_icon_routes_registered(self):
        """Both icon routes are registered when both files exist."""
        app = self._create_app_with_static_dir({"kasal-icon-16.png", "kasal-icon-24.png"})
        routes = [r.path for r in app.routes]
        assert "/kasal-icon-16.png" in routes
        assert "/kasal-icon-24.png" in routes

    def test_icon_24_route_not_registered_when_file_missing(self):
        """kasal-icon-24.png route is NOT registered when file doesn't exist."""
        app = self._create_app_with_static_dir({"kasal-icon-16.png"})
        routes = [r.path for r in app.routes]
        assert "/kasal-icon-24.png" not in routes

    def test_icon_16_route_not_registered_when_file_missing(self):
        """kasal-icon-16.png route is NOT registered when file doesn't exist."""
        app = self._create_app_with_static_dir({"kasal-icon-24.png"})
        routes = [r.path for r in app.routes]
        assert "/kasal-icon-16.png" not in routes


class TestEntrypointIconRouteInSource:
    """Verify entrypoint.py source code contains the kasal-icon-24 route."""

    def test_entrypoint_serves_kasal_icon_24(self):
        """entrypoint.py must contain a route for kasal-icon-24.png."""
        import pathlib
        entrypoint_path = pathlib.Path(__file__).resolve().parents[3] / "entrypoint.py"
        source = entrypoint_path.read_text()
        assert "kasal-icon-24.png" in source
        assert "serve_kasal_icon_24" in source

    def test_entrypoint_serves_kasal_icon_16(self):
        """entrypoint.py must contain a route for kasal-icon-16.png."""
        import pathlib
        entrypoint_path = pathlib.Path(__file__).resolve().parents[3] / "entrypoint.py"
        source = entrypoint_path.read_text()
        assert "kasal-icon-16.png" in source
        assert "serve_kasal_icon_16" in source
