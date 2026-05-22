"""
duckdb_metrics.py
=================
Minimal Prometheus exporter for PSX Analytics DuckDB health metrics.
Exposes row counts, WAL presence, and last pipeline run timestamp.

Run standalone: python prometheus/exporters/duckdb_metrics.py
Scrape port:    9101
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    import duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False

DUCKDB_PATH = Path(os.getenv("PSX_DATA_ROOT", "/opt/airflow/data")) / "psx_analytics.duckdb"
EXPORTER_PORT = int(os.getenv("DUCKDB_EXPORTER_PORT", "9101"))


def _collect() -> str:
    """Collect DuckDB metrics and return Prometheus text format."""
    lines: list[str] = []

    # ── WAL presence ──────────────────────────────────────────────────────────
    wal_path = Path(str(DUCKDB_PATH) + ".wal")
    wal_present = 1 if wal_path.exists() else 0
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
    lines.append("# HELP duckdb_wal_present 1 if a WAL file exists (potential lock issue)")
    lines.append("# TYPE duckdb_wal_present gauge")
    lines.append(f"duckdb_wal_present {wal_present}")
    lines.append("# HELP duckdb_wal_bytes_total WAL file size in bytes")
    lines.append("# TYPE duckdb_wal_bytes_total gauge")
    lines.append(f"duckdb_wal_bytes_total {wal_bytes}")

    # ── DB file size ──────────────────────────────────────────────────────────
    db_bytes = DUCKDB_PATH.stat().st_size if DUCKDB_PATH.exists() else 0
    lines.append("# HELP duckdb_db_bytes_total DuckDB file size in bytes")
    lines.append("# TYPE duckdb_db_bytes_total gauge")
    lines.append(f"duckdb_db_bytes_total {db_bytes}")

    # ── Row counts (read_only connection) ─────────────────────────────────────
    if _DUCKDB_AVAILABLE and DUCKDB_PATH.exists():
        try:
            conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
            for table in ("fact_daily_analytics", "dim_symbol", "dim_session"):
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    lines.append(f"# HELP duckdb_table_rows_total Row count for {table}")
                    lines.append("# TYPE duckdb_table_rows_total gauge")
                    lines.append(f'duckdb_table_rows_total{{table="{table}"}} {count}')
                except Exception:
                    lines.append(f'duckdb_table_rows_total{{table="{table}"}} -1')
            conn.close()
        except Exception as exc:
            lines.append(f"# ERROR: could not open DuckDB: {exc}")
    else:
        lines.append("# INFO: DuckDB file absent or duckdb package unavailable")

    # ── Scrape timestamp ──────────────────────────────────────────────────────
    lines.append("# HELP duckdb_exporter_last_scrape_unix Last scrape time (Unix seconds)")
    lines.append("# TYPE duckdb_exporter_last_scrape_unix gauge")
    lines.append(f"duckdb_exporter_last_scrape_unix {int(time.time())}")

    return "\n".join(lines) + "\n"


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/metrics":
            body = _collect().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt: str, *args: object) -> None:  # noqa: N802
        pass  # suppress default HTTP server log spam


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", EXPORTER_PORT), _Handler)
    print(f"DuckDB metrics exporter listening on :{EXPORTER_PORT}/metrics")
    server.serve_forever()
