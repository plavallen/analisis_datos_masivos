"""Importa el export del dashboard en Superset sobrescribiendo todo (conexión, datasets, charts y dashboard).

`superset import-dashboards` solo sobrescribe el dashboard: los charts y datasets que ya existen
se conservan sin cambios. Este script usa POST /api/v1/assets/import/, que sí los reemplaza.

Uso:
    python3 scripts/superset_importar.py <export.zip>

Variables de entorno: SUPERSET_URL (http://localhost:8088), SUPERSET_ADMIN_USER, SUPERSET_ADMIN_PASS.
Solo usa la biblioteca estándar (corre con el python3 del sistema).
"""
import io
import json
import os
import sys
import urllib.error
import urllib.request
import uuid
import zipfile

URL = os.environ.get("SUPERSET_URL", "http://localhost:8088").rstrip("/")
USER = os.environ.get("SUPERSET_ADMIN_USER") or "admin"
PASS = os.environ.get("SUPERSET_ADMIN_PASS") or "admin"


def pedir(req):
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        sys.exit(f"{req.get_method()} {req.full_url} → {e.code}: {e.read().decode()[:800]}")


def como_assets(zip_path):
    """Copia el zip cambiando metadata.yaml a type: assets (lo exige /api/v1/assets/import/)."""
    salida = io.BytesIO()
    with zipfile.ZipFile(zip_path) as zin, zipfile.ZipFile(salida, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename.endswith("/metadata.yaml"):
                data = data.replace(b"type: Dashboard", b"type: assets")
            zout.writestr(item, data)
        bases = [n.split("/", 1)[1] for n in zin.namelist() if n.split("/", 1)[1].startswith("databases/")]
    return salida.getvalue(), bases


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    bundle, bases = como_assets(sys.argv[1])

    login = urllib.request.Request(
        f"{URL}/api/v1/security/login",
        data=json.dumps({"username": USER, "password": PASS, "provider": "db", "refresh": True}).encode(),
        headers={"Content-Type": "application/json"},
    )
    token = pedir(login)["access_token"]

    # multipart/form-data: el bundle y las contraseñas de las conexiones (Doris usa root sin contraseña)
    limite = uuid.uuid4().hex
    partes = [
        (f'--{limite}\r\nContent-Disposition: form-data; name="bundle"; filename="bundle.zip"\r\n'
         "Content-Type: application/zip\r\n\r\n").encode() + bundle + b"\r\n",
        (f'--{limite}\r\nContent-Disposition: form-data; name="passwords"\r\n\r\n'
         + json.dumps({b: "" for b in bases}) + "\r\n").encode(),
        f"--{limite}--\r\n".encode(),
    ]
    imp = urllib.request.Request(
        f"{URL}/api/v1/assets/import/",
        data=b"".join(partes),
        headers={"Authorization": f"Bearer {token}", "Content-Type": f"multipart/form-data; boundary={limite}"},
        method="POST",
    )
    print(pedir(imp))


if __name__ == "__main__":
    main()
