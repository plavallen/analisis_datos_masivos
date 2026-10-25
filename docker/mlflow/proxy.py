"""Proxy HTTP en 0.0.0.0:5001 -> 127.0.0.1:5001.

Reescribe el header Host de las peticiones entrantes a 'localhost:5001'
para evitar el rechazo por DNS-rebinding del servidor MLflow.
"""
import threading
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.request
import urllib.error


BACKEND = "http://127.0.0.1:5001"


class ProxyHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # silenciar logs por defecto

    def _proxy(self):
        # Construir URL del backend
        url = BACKEND + self.path

        # Copiar headers, reemplazando Host
        headers = {}
        for key, value in self.headers.items():
            if key.lower() == "host":
                headers["Host"] = "localhost:5001"
            else:
                headers[key] = value

        # Leer body si existe
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else None

        req = urllib.request.Request(url, data=body, headers=headers, method=self.command)
        try:
            with urllib.request.urlopen(req) as resp:
                self.send_response(resp.status)
                for key, value in resp.headers.items():
                    if key.lower() not in ("transfer-encoding",):
                        self.send_header(key, value)
                self.end_headers()
                self.wfile.write(resp.read())
        except urllib.error.HTTPError as e:
            self.send_response(e.code)
            for key, value in e.headers.items():
                if key.lower() not in ("transfer-encoding",):
                    self.send_header(key, value)
            self.end_headers()
            self.wfile.write(e.read())

    def do_GET(self):    self._proxy()
    def do_POST(self):   self._proxy()
    def do_PUT(self):    self._proxy()
    def do_DELETE(self): self._proxy()
    def do_PATCH(self):  self._proxy()
    def do_HEAD(self):   self._proxy()
    def do_OPTIONS(self): self._proxy()


if __name__ == "__main__":
    # Este proxy ya no se usa: MLflow escucha directamente en 0.0.0.0:5001
    print("proxy.py: no se usa en esta configuración.", flush=True)
