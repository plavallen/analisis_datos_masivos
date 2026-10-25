"""Empaca y desempaca el export del dashboard de Superset.

La carpeta superset/olist_dashboard/ (YAML) es la fuente de verdad del dashboard;
el zip se genera desde ella para importarlo en Superset.

Uso:
    python3 scripts/superset_bundle.py desempacar <export.zip> <carpeta>
    python3 scripts/superset_bundle.py empacar    <carpeta>    <salida.zip>

desempacar: reemplaza el contenido de <carpeta> con el export, sin la carpeta raíz con
            fecha (dashboard_export_AAAAMMDDTHHMMSS/) y sin el sufijo _<id> de los nombres
            de archivo, para que git muestre solo cambios reales entre exports.
empacar:    genera el zip con raíz olist_dashboard/, entradas ordenadas y fecha fija,
            de modo que la misma carpeta produce siempre el mismo zip.
"""
import re
import shutil
import sys
import zipfile
from pathlib import Path

RAIZ_ZIP = "olist_dashboard"
FECHA_FIJA = (1980, 1, 1, 0, 0, 0)
SUFIJO_ID = re.compile(r"_\d+(\.yaml)$")


def desempacar(zip_path, carpeta):
    carpeta = Path(carpeta)
    with zipfile.ZipFile(zip_path) as z:
        archivos = [i for i in z.infolist() if not i.is_dir()]
        destinos = {}
        for item in archivos:
            # Se quita la carpeta raíz del export (dashboard_export_<fecha>/)
            relativo = item.filename.split("/", 1)[1]
            limpio = SUFIJO_ID.sub(r"\1", relativo)
            if limpio in destinos.values():
                limpio = relativo  # dos archivos con el mismo nombre: se conserva el sufijo
            destinos[item.filename] = limpio
        if "metadata.yaml" not in destinos.values():
            sys.exit(f"{zip_path} no parece un export de Superset (falta metadata.yaml)")
        dashboards = [d for d in destinos.values() if d.startswith("dashboards/")]
        if len(dashboards) != 1:
            # superset export-dashboards exporta todos los dashboards de la instancia
            print(f"AVISO: el export trae {len(dashboards)} dashboards: {dashboards}")

        if carpeta.exists():
            shutil.rmtree(carpeta)
        for item in archivos:
            destino = carpeta / destinos[item.filename]
            destino.parent.mkdir(parents=True, exist_ok=True)
            destino.write_bytes(z.read(item.filename))
    print(f"{len(archivos)} archivos en {carpeta}/")


def empacar(carpeta, zip_path):
    carpeta = Path(carpeta)
    if not (carpeta / "metadata.yaml").exists():
        sys.exit(f"{carpeta}/metadata.yaml no existe")
    archivos = sorted(p for p in carpeta.rglob("*") if p.is_file() and p.suffix == ".yaml")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for p in archivos:
            info = zipfile.ZipInfo(f"{RAIZ_ZIP}/{p.relative_to(carpeta).as_posix()}", FECHA_FIJA)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            z.writestr(info, p.read_bytes())
    print(f"{len(archivos)} archivos en {zip_path}")


if __name__ == "__main__":
    acciones = {"desempacar": desempacar, "empacar": empacar}
    if len(sys.argv) != 4 or sys.argv[1] not in acciones:
        sys.exit(__doc__)
    acciones[sys.argv[1]](sys.argv[2], sys.argv[3])
