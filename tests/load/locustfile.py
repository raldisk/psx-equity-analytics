"""
locustfile.py — P3 PSX Analytics API load test.
Target: p95 latency < 200ms at 50 concurrent users.
Run: locust -f tests/load/locustfile.py --host=http://localhost:8000
"""
from locust import HttpUser, task, between


class PSXApiUser(HttpUser):
    wait_time = between(0.5, 2.0)

    @task(5)
    def get_analytics(self):
        """Primary endpoint — most frequent pattern."""
        self.client.get(
            "/analytics/SM",
            params={"from": "2024-01-01", "to": "2024-03-31"},
            name="/analytics/{symbol}",
        )

    @task(2)
    def get_symbols(self):
        self.client.get("/symbols", name="/symbols")

    @task(1)
    def health_check(self):
        self.client.get("/health", name="/health")
