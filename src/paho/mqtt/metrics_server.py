"""
HTTP server for exposing MQTT Prometheus metrics.

This module provides a simple HTTP server that exposes MQTT client metrics
in Prometheus format for scraping.
"""
from __future__ import annotations

import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import TYPE_CHECKING

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, REGISTRY

if TYPE_CHECKING:
    from typing import Optional


class MetricsHTTPHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Prometheus metrics endpoint."""

    def do_GET(self) -> None:
        if self.path == '/metrics':
            metrics_data = generate_latest(REGISTRY)
            self.send_response(200)
            self.send_header('Content-Type', CONTENT_TYPE_LATEST)
            self.send_header('Content-Length', str(len(metrics_data)))
            self.end_headers()
            self.wfile.write(metrics_data)
        elif self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args) -> None:
        """Suppress default logging."""
        pass


class MetricsServer:
    """HTTP server for exposing MQTT metrics to Prometheus."""

    def __init__(self, host: str = '0.0.0.0', port: int = 9000) -> None:
        self.host = host
        self.port = port
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the metrics HTTP server in a background thread."""
        self._server = HTTPServer((self.host, self.port), MetricsHTTPHandler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the metrics HTTP server."""
        if self._server:
            self._server.shutdown()
            self._server = None
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def is_running(self) -> bool:
        """Check if the server is running."""
        return self._server is not None and self._thread is not None


_metrics_server: MetricsServer | None = None


def start_metrics_server(host: str = '0.0.0.0', port: int = 9000) -> MetricsServer:
    """Start the global metrics server.
    
    :param host: Host to bind the server to
    :param port: Port to bind the server to
    :returns: The MetricsServer instance
    """
    global _metrics_server
    if _metrics_server is None or not _metrics_server.is_running():
        _metrics_server = MetricsServer(host=host, port=port)
        _metrics_server.start()
    return _metrics_server


def stop_metrics_server() -> None:
    """Stop the global metrics server."""
    global _metrics_server
    if _metrics_server:
        _metrics_server.stop()
        _metrics_server = None


def get_metrics_server() -> MetricsServer | None:
    """Get the current metrics server instance."""
    return _metrics_server
