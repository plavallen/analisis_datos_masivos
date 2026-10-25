import sys, json, requests

TOKEN = sys.argv[1]
BASE  = "http://localhost:8088"
HEADS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api(method, path, **kw):
    r = requests.request(method, f"{BASE}{path}", headers=HEADS, **kw)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"{method} {path} → {r.status_code}: {r.text[:400]}")
    return r.json()

# ── 1. Actualizar chart 32 (tabla) ────────────────────────────────────────────
# Columnas a mostrar (sin puntaje, sin percentil)
COLS = ["order_id", "seller_id", "ruta", "categoria",
        "fecha_prometida", "dias_hasta_promesa", "nivel_riesgo", "riesgo_relativo"]

params = {
    "viz_type": "table",
    "all_columns": COLS,
    "order_desc": True,
    "order_by_cols": ["[\"riesgo_relativo\", false]"],
    "page_length": 25,
    "include_search": True,
    "show_cell_bars": False,
    "table_timestamp_format": "%Y-%m-%d",
    # Colores por string equality en nivel_riesgo (sobrios, sin blanco)
    "conditional_formatting": [
        {"column": "nivel_riesgo", "operator": "=", "targetValue": "alto",
         "colorScheme": "#F8D7DA"},
        {"column": "nivel_riesgo", "operator": "=", "targetValue": "medio",
         "colorScheme": "#FFF3CD"},
        {"column": "nivel_riesgo", "operator": "=", "targetValue": "bajo",
         "colorScheme": "#D4EDDA"},
    ],
    "column_config": {
        "order_id":           {"label": "Pedido",            "columnWidth": 90},
        "seller_id":          {"label": "Vendedor",          "columnWidth": 80},
        "ruta":               {"label": "Ruta",              "columnWidth": 100},
        "categoria":          {"label": "Categoría",         "columnWidth": 160},
        "fecha_prometida":    {"label": "Fecha prometida",   "columnWidth": 110},
        "dias_hasta_promesa": {"label": "Días",              "columnWidth": 55,
                               "d3NumberFormat": ",.0f"},
        "nivel_riesgo":       {"label": "Riesgo",            "columnWidth": 80},
        "riesgo_relativo":    {"label": "Riesgo relativo",   "columnWidth": 100,
                               "d3NumberFormat": ".2f"},
    },
    "metrics": [],
    "groupby": [],
    "row_limit": 1826,
}

api("PUT", "/api/v1/chart/32", json={
    "slice_name":      "Pedidos en curso con mayor riesgo de atraso",
    "viz_type":        "table",
    "datasource_id":   21,
    "datasource_type": "table",
    "params":          json.dumps(params),
    "query_context":   json.dumps({
        "datasource": {"id": 21, "type": "table"},
        "queries": [{
            "columns": COLS,
            "orderby": [["riesgo_relativo", False]],
            "row_limit": 1826,
        }],
        "result_format": "json",
        "result_type": "full",
    }),
})
print("Chart 32 actualizado OK")

# ── 2. Limpiar layout: quitar markdown, dejar solo KPI en ROW-RIESGO-NOTA ─────
r = requests.get(f"{BASE}/api/v1/dashboard/olist", headers=HEADS)
data = r.json()["result"]
dashboard_id = data["id"]
pos = json.loads(data["position_json"])

# Eliminar markdown y columna de markdown
for key in ["MARKDOWN-RIESGO-NOTA", "COLUMN-RIESGO-MD"]:
    pos.pop(key, None)

# Actualizar ROW-RIESGO-NOTA: solo tiene la columna KPI, que pasa a ancho completo
if "ROW-RIESGO-NOTA" in pos:
    pos["ROW-RIESGO-NOTA"]["children"] = ["COLUMN-RIESGO-KPI"]

# Expandir columna KPI a ancho 6 (era 3)
if "COLUMN-RIESGO-KPI" in pos:
    pos["COLUMN-RIESGO-KPI"]["meta"]["width"] = 6

r2 = requests.put(f"{BASE}/api/v1/dashboard/{dashboard_id}", headers=HEADS,
                  json={"position_json": json.dumps(pos)})
print(f"Layout PUT → {r2.status_code}", "OK" if r2.status_code in (200,201) else r2.text[:200])
