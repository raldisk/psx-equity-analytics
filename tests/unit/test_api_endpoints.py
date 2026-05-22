"""
test_api_endpoints.py
=====================
FastAPI endpoint unit tests via httpx TestClient.
Validates F-022 enforcement (date range required), /health, and error handling.
"""
from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def client():
    """Create a TestClient for the PSX analytics API."""
    from fastapi.testclient import TestClient
    from serving.psx_analytics_api import app
    return TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_ok_status(self, client):
        data = client.get("/health").json()
        assert data.get("status") == "ok"

    def test_health_identifies_service(self, client):
        data = client.get("/health").json()
        assert "psx" in data.get("service", "").lower()


class TestDateRangeEnforcement:
    """F-022: All fact-table endpoints must reject missing or invalid date ranges."""

    def test_daily_analytics_missing_dates_raises(self, client):
        """Endpoint must reject requests with no date range."""
        response = client.get("/analytics/daily?symbol=SM")
        # Missing required dates must return 422 or 400
        assert response.status_code in (400, 422)

    def test_daily_analytics_inverted_range_raises(self, client):
        """start_date > end_date must be rejected."""
        with patch("serving.psx_analytics_api._get_duckdb_manager") as mock_mgr:
            mock_validate = MagicMock(side_effect=ValueError("start_date > end_date"))
            mock_conn = MagicMock()
            mock_mgr.return_value = (mock_conn, mock_validate)
            response = client.get(
                "/analytics/daily?symbol=SM&start_date=2024-03-31&end_date=2024-01-01"
            )
            assert response.status_code in (400, 422)
