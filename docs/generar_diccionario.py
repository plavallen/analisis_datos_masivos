"""Genera docs/diccionario_datos.md desde docs/diccionario.yaml."""

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent
YAML_PATH = ROOT / "docs" / "diccionario.yaml"
MD_PATH = ROOT / "docs" / "diccionario_datos.md"


def main():
    with open(YAML_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    lines = ["# Diccionario de datos\n"]

    for tabla_nombre, tabla in data.get("tablas", {}).items():
        capa = tabla.get("capa", "")
        desc = tabla.get("descripcion", "")
        lines.append(f"\n## `{tabla_nombre}`")
        if capa:
            lines.append(f"**Capa:** {capa}")
        if desc:
            lines.append(f"\n{desc}\n")

        columnas = tabla.get("columnas", {})
        if columnas:
            lines.append("| Columna | Tipo | Descripción | Origen | Disponible al comprar |")
            lines.append("|---|---|---|---|---|")
            for col, meta in columnas.items():
                tipo = meta.get("tipo", "")
                descripcion = meta.get("descripcion", "").replace("|", "\\|")
                origen = meta.get("origen", "")
                disponible = "sí" if meta.get("disponible_al_comprar") else "no"
                lines.append(f"| `{col}` | {tipo} | {descripcion} | {origen} | {disponible} |")

    MD_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Generado: {MD_PATH}")


if __name__ == "__main__":
    main()
