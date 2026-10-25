"""
Rediseño completo de la sección "Pedidos en curso con mayor riesgo de atraso"
en la Vista de decisión del dashboard Olist.

Layout final:
  ROW-HEADER:  Markdown a todo el ancho (encabezado + nota)
  ROW-CHARTS:  ChartA(4) + ChartB(4) + ChartC(4)   → 3 gráficos
  ROW-TABLE:   Tabla (ancho 12)
"""
import sys, json, requests

TOKEN = sys.argv[1]
BASE  = "http://localhost:8088"
HEADS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
DS_ID = 21

CUTOFF       = "2018-06-15"
CUTOFF_LABEL = "15/06/2018"

C_VENCIDO = "#8A3A10"
C_ALTO    = "#B8571B"
C_MEDIO   = "#E3A27A"
C_BAJO    = "#C9CED6"
# Versiones claras para conditional formatting
CL_VENCIDO = "#F5CBAA"
CL_ALTO    = "#FAE0CC"
CL_MEDIO   = "#FDEBD9"
CL_BAJO    = "#EDEEF0"


def api(method, path, raise_on_error=True, **kw):
    r = requests.request(method, f"{BASE}{path}", headers=HEADS, **kw)
    if raise_on_error and r.status_code not in (200, 201, 204):
        raise RuntimeError(f"{method} {path} → {r.status_code}: {r.text[:500]}")
    return r


# ── 1. Actualizar SQL del dataset 21 ──────────────────────────────────────────
NEW_SQL = """
SELECT
    order_id,
    LEFT(order_id, 8)   AS pedido,
    seller_id,
    LEFT(seller_id, 8)  AS vendedor,
    ruta,
    categoria,
    fecha_prometida,
    dias_hasta_promesa,
    puntaje,
    percentil_puntaje,
    nivel_riesgo,
    riesgo_num,
    riesgo_relativo,
    semana_prometida,
    version_modelo,
    fecha_corte
FROM riesgo_pedidos
""".strip()

api("PUT", f"/api/v1/dataset/{DS_ID}", json={"sql": NEW_SQL, "schema": "olist_bi"})
print("Dataset SQL actualizado")

# Forzar actualización de columnas
api("PUT", f"/api/v1/dataset/{DS_ID}/refresh")
print("Dataset refreshed")


# ── 2. Eliminar Chart 33 (Big Number) ────────────────────────────────────────
r = api("DELETE", "/api/v1/chart/33", raise_on_error=False)
print(f"Chart 33 eliminado: {r.status_code}")


# ── 3. Crear Chart A: Pedidos por nivel de riesgo (barras horizontales) ───────
def make_horiz_bar(title, groupby, filters=None, row_limit=10, order_bars=True):
    params = {
        "viz_type":      "horiz_bar",
        "metrics":       [{"expressionType": "COUNT", "label": "Pedidos"}],
        "groupby":       groupby,
        "bar_stacked":   False,
        "order_bars":    order_bars,
        "show_bar_value": True,
        "show_legend":   False,
        "y_axis_format": ",.0f",
        "adhoc_filters": filters or [],
        "row_limit":     row_limit,
        "color_scheme":  "custom",
        "label_colors":  {
            "Vencido": C_VENCIDO,
            "Alto":    C_ALTO,
            "Medio":   C_MEDIO,
            "Bajo":    C_BAJO,
        },
    }
    r = api("POST", "/api/v1/chart/", json={
        "slice_name":      title,
        "viz_type":        "horiz_bar",
        "datasource_id":   DS_ID,
        "datasource_type": "table",
        "params":          json.dumps(params),
        "dashboards":      [1],
    })
    return r.json()["id"]


CHART_A_ID = make_horiz_bar(
    "Pedidos en curso por nivel de riesgo",
    ["nivel_riesgo"],
    row_limit=4,
    order_bars=False,
)
print(f"Chart A id: {CHART_A_ID}")


# ── 4. Crear Chart B: Rutas con más pedidos en riesgo alto ────────────────────
FILTER_ALTO = {
    "clause": "WHERE",
    "expressionType": "SIMPLE",
    "subject":    "nivel_riesgo",
    "operator":   "==",
    "comparator": "Alto",
}
CHART_B_PARAMS = {
    "viz_type":      "horiz_bar",
    "metrics":       [{"expressionType": "COUNT", "label": "Pedidos"}],
    "groupby":       ["ruta"],
    "bar_stacked":   False,
    "order_bars":    True,
    "show_bar_value": True,
    "show_legend":   False,
    "y_axis_format": ",.0f",
    "adhoc_filters": [FILTER_ALTO],
    "row_limit":     8,
    "color_scheme":  "preset_colors",
    "label_colors":  {},
}
r_b = api("POST", "/api/v1/chart/", json={
    "slice_name":      "Rutas con más pedidos en riesgo alto",
    "viz_type":        "horiz_bar",
    "datasource_id":   DS_ID,
    "datasource_type": "table",
    "params":          json.dumps(CHART_B_PARAMS),
    "dashboards":      [1],
})
CHART_B_ID = r_b.json()["id"]
print(f"Chart B id: {CHART_B_ID}")


# ── 5. Crear Chart C: Pedidos en riesgo alto por semana prometida ─────────────
CHART_C_PARAMS = {
    "viz_type":      "bar",
    "metrics":       [{"expressionType": "COUNT", "label": "Pedidos"}],
    "groupby":       ["semana_prometida"],
    "bar_stacked":   False,
    "order_bars":    False,
    "show_bar_value": True,
    "show_legend":   False,
    "y_axis_format": ",.0f",
    "adhoc_filters": [FILTER_ALTO],
    "row_limit":     52,
    "color_scheme":  "preset_colors",
    "label_colors":  {"semana_prometida": C_ALTO},
    "bottom_margin": "auto",
    "x_ticks_layout": "auto",
}
r_c = api("POST", "/api/v1/chart/", json={
    "slice_name":      "Pedidos en riesgo alto por semana prometida",
    "viz_type":        "bar",
    "datasource_id":   DS_ID,
    "datasource_type": "table",
    "params":          json.dumps(CHART_C_PARAMS),
    "dashboards":      [1],
})
CHART_C_ID = r_c.json()["id"]
print(f"Chart C id: {CHART_C_ID}")


# ── 6. Actualizar Chart 32 (tabla) ────────────────────────────────────────────
# Incluye riesgo_num solo para ordenar (ancho mínimo para ocultarlo visualmente)
COLS_TABLE = [
    "riesgo_num", "nivel_riesgo", "riesgo_relativo",
    "dias_hasta_promesa", "ruta", "categoria",
    "vendedor", "pedido", "fecha_prometida",
]
TABLE_PARAMS = {
    "viz_type":     "table",
    "all_columns":  COLS_TABLE,
    "order_desc":   True,
    "order_by_cols": ['["riesgo_num", false]', '["riesgo_relativo", false]'],
    "page_length":  15,
    "include_search": True,
    "show_cell_bars": False,
    "table_timestamp_format": "%Y-%m-%d",
    "conditional_formatting": [
        {"column": "nivel_riesgo", "operator": "=", "targetValue": "Vencido",
         "colorScheme": CL_VENCIDO},
        {"column": "nivel_riesgo", "operator": "=", "targetValue": "Alto",
         "colorScheme": CL_ALTO},
        {"column": "nivel_riesgo", "operator": "=", "targetValue": "Medio",
         "colorScheme": CL_MEDIO},
        {"column": "nivel_riesgo", "operator": "=", "targetValue": "Bajo",
         "colorScheme": CL_BAJO},
        {"column": "dias_hasta_promesa", "operator": "<=", "targetValue": 3,
         "colorScheme": "#FFB347"},
    ],
    "column_config": {
        "riesgo_num":        {"label": "",               "columnWidth": 1},
        "nivel_riesgo":      {"label": "Nivel",          "columnWidth": 70},
        "riesgo_relativo":   {"label": "Riesgo relativo","columnWidth": 95,
                              "d3NumberFormat": ".1f", "showCellBars": True},
        "dias_hasta_promesa":{"label": "Días",           "columnWidth": 48},
        "ruta":              {"label": "Ruta",           "columnWidth": 80},
        "categoria":         {"label": "Categoría",      "columnWidth": 110},
        "vendedor":          {"label": "Vendedor",       "columnWidth": 62},
        "pedido":            {"label": "Pedido",         "columnWidth": 62},
        "fecha_prometida":   {"label": "Fecha prometida","columnWidth": 100},
    },
    "metrics": [],
    "groupby": [],
    "row_limit": 1826,
}
api("PUT", "/api/v1/chart/32", json={
    "slice_name":      "Pedidos en curso con mayor riesgo de atraso",
    "viz_type":        "table",
    "datasource_id":   DS_ID,
    "datasource_type": "table",
    "params":          json.dumps(TABLE_PARAMS),
    "query_context":   json.dumps({
        "datasource": {"id": DS_ID, "type": "table"},
        "queries": [{
            "columns": COLS_TABLE,
            "orderby": [["riesgo_num", False], ["riesgo_relativo", False]],
            "row_limit": 1826,
        }],
        "result_format": "json", "result_type": "full",
    }),
})
print("Chart 32 (tabla) actualizado")


# ── 7. Actualizar layout ──────────────────────────────────────────────────────
r_db = requests.get(f"{BASE}/api/v1/dashboard/olist", headers=HEADS)
data = r_db.json()["result"]
dashboard_id = data["id"]
pos = json.loads(data["position_json"])

tab_decision = next(
    k for k, v in pos.items()
    if isinstance(v, dict) and v.get("type") == "TAB"
    and "decisi" in v.get("meta", {}).get("text", "").lower()
)
print(f"Tab decisión: {tab_decision}")

# Limpiar nodos viejos de la sección riesgo
OLD_NODES = [
    "CHART-RIESGO-KPI", "COLUMN-RIESGO-KPI", "ROW-RIESGO-NOTA",
    "ROW-RIESGO-TABLA", "CHART-RIESGO-TABLA",
    "CHART-RIESGO-TABLA-NEW", "COLUMN-RIESGO-TABLA", "ROW-RIESGO-TABLA-NEW",
]
for key in OLD_NODES:
    pos.pop(key, None)

# Filtrar children del tab
pos[tab_decision]["children"] = [
    c for c in pos[tab_decision].get("children", [])
    if c not in OLD_NODES
]

# ── Fila 1: Encabezado markdown ───────────────────────────────────────────────
MD_HDR  = "MARKDOWN-RIESGO-HDR"
COL_HDR = "COLUMN-RIESGO-HDR"
ROW_HDR = "ROW-RIESGO-HDR"

HEADER_MD = (
    "## Pedidos en curso con mayor riesgo de atraso\n\n"
    f"El nivel de riesgo ordena los pedidos según el modelo. "
    f"No es una probabilidad calibrada. Fecha de corte: {CUTOFF_LABEL}."
)

pos[MD_HDR]  = {"type": "MARKDOWN", "id": MD_HDR, "children": [],
                "parents": [tab_decision, ROW_HDR, COL_HDR],
                "meta": {"code": HEADER_MD, "width": 24, "height": 50}}
pos[COL_HDR] = {"type": "COLUMN", "id": COL_HDR,
                "children": [MD_HDR],
                "parents": [tab_decision, ROW_HDR],
                "meta": {"background": "BACKGROUND_TRANSPARENT", "width": 24}}
pos[ROW_HDR] = {"type": "ROW", "id": ROW_HDR,
                "children": [COL_HDR],
                "parents": [tab_decision],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}}

# ── Fila 2: Tres gráficos (ancho 4 c/u) ──────────────────────────────────────
ROW_GRF = "ROW-RIESGO-GRAFICOS"
chart_specs = [
    ("CHART-RIESGO-A", "COLUMN-RIESGO-A", CHART_A_ID,
     "Pedidos en curso por nivel de riesgo"),
    ("CHART-RIESGO-B", "COLUMN-RIESGO-B", CHART_B_ID,
     "Rutas con más pedidos en riesgo alto"),
    ("CHART-RIESGO-C", "COLUMN-RIESGO-C", CHART_C_ID,
     "Pedidos en riesgo alto por semana prometida"),
]
col_ids = []
for chart_node, col_id, chart_id, name in chart_specs:
    pos[chart_node] = {
        "type": "CHART", "id": chart_node, "children": [],
        "parents": [tab_decision, ROW_GRF, col_id],
        "meta": {"chartId": chart_id, "sliceName": name, "width": 4, "height": 150},
    }
    pos[col_id] = {
        "type": "COLUMN", "id": col_id,
        "children": [chart_node],
        "parents": [tab_decision, ROW_GRF],
        "meta": {"background": "BACKGROUND_TRANSPARENT", "width": 4},
    }
    col_ids.append(col_id)

pos[ROW_GRF] = {
    "type": "ROW", "id": ROW_GRF,
    "children": col_ids,
    "parents": [tab_decision],
    "meta": {"background": "BACKGROUND_TRANSPARENT"},
}

# ── Fila 3: Tabla (ancho 12) ──────────────────────────────────────────────────
CHART_TBL_NODE = "CHART-RIESGO-TABLA-V2"
COL_TBL        = "COLUMN-RIESGO-TABLA-V2"
ROW_TBL        = "ROW-RIESGO-TABLA-V2"

pos[CHART_TBL_NODE] = {
    "type": "CHART", "id": CHART_TBL_NODE, "children": [],
    "parents": [tab_decision, ROW_TBL, COL_TBL],
    "meta": {"chartId": 32,
             "sliceName": "Pedidos en curso con mayor riesgo de atraso",
             "width": 12, "height": 400},
}
pos[COL_TBL] = {
    "type": "COLUMN", "id": COL_TBL,
    "children": [CHART_TBL_NODE],
    "parents": [tab_decision, ROW_TBL],
    "meta": {"background": "BACKGROUND_TRANSPARENT", "width": 12},
}
pos[ROW_TBL] = {
    "type": "ROW", "id": ROW_TBL,
    "children": [COL_TBL],
    "parents": [tab_decision],
    "meta": {"background": "BACKGROUND_TRANSPARENT"},
}

# Insertar las 3 filas al inicio del tab de decisión
pos[tab_decision]["children"] = (
    [ROW_HDR, ROW_GRF, ROW_TBL]
    + pos[tab_decision]["children"]
)

r2 = requests.put(f"{BASE}/api/v1/dashboard/{dashboard_id}", headers=HEADS,
                  json={"position_json": json.dumps(pos)})
print(f"Layout PUT → {r2.status_code}",
      "OK" if r2.status_code in (200, 201) else r2.text[:300])

# ── Verificación ──────────────────────────────────────────────────────────────
r3 = requests.get(f"{BASE}/api/v1/dashboard/olist", headers=HEADS)
pos3 = json.loads(r3.json()["result"]["position_json"])
print(f"Tab decisión children: {pos3.get(tab_decision,{}).get('children')}")
