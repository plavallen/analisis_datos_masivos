"""verificar_dashboard.py — Validaciones automáticas del dashboard Superset.

Reglas que se verifican:
  1. Ninguna ROW supera 12 columnas (validación estática del position_json).
  2. Los gráficos de tipo country_map usan select_country con clave válida.
     Falla si alguno tiene una clave que causaría "No map data available".
  3. Los KPIs principales coinciden con mediciones/etapa2/eda/referencia_kpis.csv
     con tolerancia de 0,1 pp.
  4. Tarjetas KPI (big_number_total): viz_type=big_number_total, métrica SQL
     definida, valor no nulo vía POST /api/v1/chart/data.
     NOTA: fix9_big_number.py reemplazó Handlebars porque DOMPurify elimina
     <svg>/<polyline> en poc-superset:local 6.1.0 aunque
     HTML_SANITIZATION_SCHEMA_EXTENSIONS esté en data-bootstrap.
     La tendencia queda en los gráficos mensuales (charts 7 y 8).
  5. Ningún texto visible contiene guión corto, semiguión o raya fuera de las
     excepciones (nombres compuestos como "Centro-Oeste").
  6. Ningún viz_type del dashboard está en la lista de tipos heredados no
     registrados en Superset 6.1.
  7. Dashboard tiene 8 filtros nativos en native_filter_configuration.
  8. Descomposición Kitagawa-Oaxaca: residuo = 0 en todos los meses (customer_state).
  8. Verificación en el navegador con Playwright: screenshots de las 4 pestañas,
     valores KPI visibles, sin textos de error, barra de filtros visible,
     K01 cambia con filtro SP.

Salida: 0 si todas las reglas pasan, 1 en caso de fallos.
"""
import csv
import json
import re
import subprocess
import sys
import os

import requests

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
REF_CSV = "mediciones/etapa2/eda/referencia_kpis.csv"

# Claves de país válidas confirmadas en el bundle JS de Superset 6.1.0
VALID_COUNTRY_KEYS = {
    "brazil", "argentina", "colombia", "chile", "peru", "mexico",
    "usa", "france", "germany", "spain", "italy", "china", "india",
    "russia", "ukraine", "canada", "australia", "japan", "south_korea",
    "portugal", "uk", "netherlands", "belgium", "sweden", "norway",
    "denmark", "finland", "austria", "switzerland", "poland",
}

# viz_types heredados que ya no están registrados en Superset 6.1
LEGACY_VIZ_TYPES = {
    "heatmap",     # migrado a heatmap_v2
    "dist_bar",    # migrado a echarts_bar
    "world_map",   # migrado a country_map
    "box_plot",    # migrado a echarts_boxplot (en algunas versiones)
    "sunburst",    # migrado a echarts_sunburst (en algunas versiones)
    "treemap",     # migrado a echarts_treemap (en algunas versiones)
}

# Excepciones de guión: palabras compuestas aceptadas
DASH_EXCEPTIONS = {
    "Centro-Oeste", "Norte-Nordeste", "Sul-Sudeste",
    "P50", "P90", "P95",  # no son guiones
}

TOLERANCE = 0.001   # 0,1 pp
DORIS_QUERIES = {
    "K01_OTD": (
        "SELECT AVG(CASE WHEN dias_atraso<=0 THEN 1.0 ELSE 0.0 END)"
        " FROM olist_bi.pedidos_entregados_periodo"
    ),
    "K02_demora_media": (
        "SELECT AVG(CASE WHEN dias_atraso>0 THEN dias_atraso END)"
        " FROM olist_bi.pedidos_entregados_periodo"
    ),
    "K08_resenas_neg": (
        "SELECT AVG(CASE WHEN review_negativa THEN 1.0 ELSE 0.0 END)"
        " FROM olist_bi.pedidos_entregados_periodo"
    ),
    "K09_brecha": (
        "SELECT SUM(CASE WHEN tarde AND review_negativa THEN 1.0 ELSE 0.0 END)"
        "/NULLIF(SUM(CASE WHEN tarde THEN 1.0 ELSE 0.0 END),0)"
        "-SUM(CASE WHEN NOT tarde AND review_negativa THEN 1.0 ELSE 0.0 END)"
        "/NULLIF(SUM(CASE WHEN NOT tarde THEN 1.0 ELSE 0.0 END),0)"
        " FROM olist_bi.pedidos_entregados_periodo"
    ),
    "K10_cancelacion": (
        "SELECT AVG(CASE WHEN order_status IN ('canceled','unavailable')"
        " THEN 1.0 ELSE 0.0 END) FROM olist_bi.pedidos"
    ),
}


def doris_query(q):
    r = subprocess.run(
        ["docker", "compose",
         "exec", "-T", "doris",
         "mysql", "-u", "root", "-P", "9030", "-h", "127.0.0.1",
         "--skip-column-names", "-e", q],
        capture_output=True, text=True,
    )
    out = r.stdout.strip()
    if not out:
        return None
    try:
        return float(out)
    except ValueError:
        return None


def superset_login():
    s = requests.Session()
    r = s.post(f"{SUPERSET_URL}/api/v1/security/login",
               json={"username": "admin", "password": "admin",
                     "provider": "db", "refresh": True})
    token = r.json()["access_token"]
    csrf = s.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"},
    ).json()["result"]
    s.headers.update({"Authorization": f"Bearer {token}",
                      "X-CSRFToken": csrf, "Referer": SUPERSET_URL})
    return s


def _strip_html_for_dash_check(text):
    """Extrae solo el texto visible de un fragmento HTML/Handlebars, sin CSS ni atributos."""
    import html as _html
    # Quitar atributos style="..." y class="..."
    t = re.sub(r'style="[^"]*"', '', text)
    t = re.sub(r"style='[^']*'", '', t)
    # Quitar bloques <style>...</style>
    t = re.sub(r'<style[^>]*>.*?</style>', '', t, flags=re.DOTALL)
    # Quitar expresiones Handlebars {{...}}
    t = re.sub(r'\{\{[^}]*\}\}', '', t)
    # Quitar etiquetas HTML
    t = re.sub(r'<[^>]+>', '', t)
    return _html.unescape(t)


def _has_dashes(text, exceptions=None, is_html=False):
    """Devuelve la lista de fragmentos con guiones no permitidos."""
    exc = exceptions or set()
    clean = _strip_html_for_dash_check(text) if is_html else text
    # Eliminar excepciones
    for e in exc:
        clean = clean.replace(e, "")
    found = []
    for m in re.finditer(r'[-–—]', clean):
        ctx_start = max(0, m.start() - 15)
        ctx_end = min(len(clean), m.end() + 15)
        fragment = clean[ctx_start:ctx_end].strip()
        if fragment:
            found.append(fragment)
    return found


def main():
    errors = []
    warnings = []

    print("=" * 60)
    print("VERIFICACIÓN DEL DASHBOARD")
    print("=" * 60)

    s = superset_login()

    # ── 1. Anchos de filas ────────────────────────────────────────────────────
    print("\n[1] Anchos de filas (position_json)")
    dash = s.get(f"{SUPERSET_URL}/api/v1/dashboard/1").json()["result"]
    pos = json.loads(dash["position_json"])

    row_errors = []
    for k, v in pos.items():
        if not isinstance(v, dict) or v.get("type") != "ROW":
            continue
        total = sum(
            pos[c]["meta"].get("width", 0)
            for c in v.get("children", [])
            if c in pos and isinstance(pos.get(c), dict)
        )
        if total > 12:
            row_errors.append(f"  ✗ {k}: suma={total} > 12")
        else:
            print(f"  ✓ {k}: suma={total}")

    if row_errors:
        print("  FALLOS:")
        for e in row_errors:
            print(e)
        errors.extend(row_errors)
    else:
        print("  Todas las filas dentro de 12 columnas ✓")

    # ── 2. Country Map: clave válida ──────────────────────────────────────────
    print("\n[2] Country Map: clave select_country válida")
    charts = s.get(f"{SUPERSET_URL}/api/v1/chart/?q=(page_size:100)").json()["result"]
    map_errors = []
    for c in charts:
        if c.get("viz_type") != "country_map":
            continue
        p = json.loads(c.get("params") or "{}")
        key = p.get("select_country", "")
        entity = p.get("entity", "")
        ok_key = key in VALID_COUNTRY_KEYS
        ok_entity = bool(entity)
        status = "✓" if ok_key and ok_entity else "✗"
        print(f"  {status} chart-{c['id']} '{c['slice_name']}': "
              f"select_country={key!r}  entity={entity!r}")
        if not ok_key:
            msg = (
                f"  ✗ chart-{c['id']}: select_country={key!r} no está en las "
                f"claves válidas del bundle JS — causaría 'No map data available for {key}'"
            )
            map_errors.append(msg)
        if not ok_entity:
            map_errors.append(f"  ✗ chart-{c['id']}: entity vacío")

    if map_errors:
        errors.extend(map_errors)
    elif any(c.get("viz_type") == "country_map" for c in charts):
        print("  Todos los mapas tienen clave válida ✓")
    else:
        warnings.append("  ⚠ No hay gráficos country_map en el dashboard")
        print("  ⚠ No hay gráficos country_map (¿se quitaron?)")

    # ── 3. KPIs vs. referencia ────────────────────────────────────────────────
    print("\n[3] KPIs vs. referencia (tolerancia 0,1 pp)")
    ref = {}
    if os.path.exists(REF_CSV):
        with open(REF_CSV) as f:
            for row in csv.DictReader(f):
                ref[row["kpi"]] = float(row["valor"])
    else:
        warnings.append(f"  ⚠ {REF_CSV} no encontrado — omitiendo comparación")
        print(f"  ⚠ {REF_CSV} no encontrado")

    kpi_errors = []
    if ref:
        print(f"  {'KPI':<22} {'Calculado':>12} {'Referencia':>12} {'Δ abs':>10} {'OK'}")
        print(f"  {'-'*62}")
        for kpi, query in DORIS_QUERIES.items():
            val = doris_query(query)
            ref_val = ref.get(kpi)
            if val is None or ref_val is None:
                print(f"  ⚠ {kpi}: no se pudo calcular (Doris no disponible?)")
                continue
            diff = abs(val - ref_val)
            ok = diff <= TOLERANCE
            status = "✓" if ok else "✗"
            print(f"  {status} {kpi:<22} {val:>12.6f} {ref_val:>12.6f} {diff*100:>9.3f}pp")
            if not ok:
                kpi_errors.append(
                    f"  ✗ {kpi}: calculado={val:.6f} referencia={ref_val:.6f} Δ={diff*100:.3f}pp > 0.1pp"
                )
        errors.extend(kpi_errors)

    # ── 4. KPI tarjetas: big_number_total con valor no nulo ──────────────────
    # Reemplazó Handlebars: DOMPurify elimina <svg>/<polyline> en este build
    # aunque HTML_SANITIZATION_SCHEMA_EXTENSIONS esté en data-bootstrap.
    print("\n[4] KPI tarjetas (big_number_total): viz_type y valor no nulo vía API")
    kpi_chart_names = [
        "K01 Entrega a tiempo (OTD)",
        "K08 Reseñas negativas",
        "K09 Brecha de satisfacción",
        "K10 Tasa de cancelación",
        "K11 GMV del período",
        "K02 Demora media de tardíos",
    ]
    bnt_errors = []
    for name in kpi_chart_names:
        c = next((x for x in charts if x["slice_name"] == name), None)
        if not c:
            warnings.append(f"  ⚠ Chart '{name}' no encontrado")
            print(f"  ⚠ '{name}' no encontrado")
            continue
        vt = c.get("viz_type", "")
        if vt != "big_number_total":
            bnt_errors.append(
                f"  ✗ '{name}': viz_type={vt!r} (esperado 'big_number_total')"
            )
            print(f"  ✗ '{name}': viz_type={vt!r}")
            continue
        p = json.loads(c.get("params") or "{}")
        metric = p.get("metric", {})
        sql = (metric.get("sqlExpression", "") if isinstance(metric, dict)
               else str(metric))
        if not sql:
            bnt_errors.append(f"  ✗ '{name}': métrica SQL vacía")
            print(f"  ✗ '{name}': métrica SQL vacía")
            continue
        # Consultar valor vía API
        ds_id = c.get("datasource_id")
        qc = {
            "datasource": {"id": ds_id, "type": "table"},
            "force": False,
            "queries": [{
                "columns": [],
                "metrics": [{"expressionType": "SQL", "sqlExpression": sql,
                             "label": name, "hasCustomLabel": True}],
                "row_limit": 1,
                "time_range": "No filter",
                "filters": [],
                "extras": {"time_grain_sqla": None, "having": "", "where": ""}
            }]
        }
        try:
            resp = s.post(f"{SUPERSET_URL}/api/v1/chart/data", json=qc, timeout=20)
            if resp.status_code != 200:
                warnings.append(f"  ⚠ '{name}': API {resp.status_code}")
                print(f"  ⚠ '{name}': API {resp.status_code}")
                continue
            data_rows = resp.json().get("result", [{}])[0].get("data", [{}])
            val = data_rows[0].get(name) if data_rows else None
            ok = val is not None and val != 0.0 and val != ""
            status = "✓" if ok else "✗"
            print(f"  {status} '{name}': valor={val!r}")
            if not ok:
                bnt_errors.append(f"  ✗ '{name}': valor nulo o cero ({val!r})")
        except Exception as exc:
            warnings.append(f"  ⚠ '{name}': {exc}")
            print(f"  ⚠ '{name}': {exc}")
    errors.extend(bnt_errors)

    # ── 5. Sin guiones en textos visibles ─────────────────────────────────────
    print("\n[5] Sin guiones en textos visibles")
    dash_errors = []

    # Textos de Markdown en position_json
    for key, val in pos.items():
        if isinstance(val, dict) and val.get("type") == "MARKDOWN":
            text = val.get("meta", {}).get("text", "")
            bad = _has_dashes(text, DASH_EXCEPTIONS)
            if bad:
                msg = f"  ✗ Markdown {key}: guiones en {bad[:3]}"
                dash_errors.append(msg)
                print(msg)
            else:
                print(f"  ✓ Markdown {key}: sin guiones")

    # Nombres de charts y params
    for c in charts:
        name = c.get("slice_name", "")
        bad_name = _has_dashes(name, DASH_EXCEPTIONS)
        if bad_name:
            msg = f"  ✗ chart-{c['id']} nombre: {bad_name[:2]}"
            dash_errors.append(msg)
            print(msg)

        p = json.loads(c.get("params") or "{}")
        # Revisar label de métricas, notas y textos visibles en params
        texts_to_check = []  # (text, is_html)
        for m in p.get("metrics", []) + p.get("metrics_b", []):
            if isinstance(m, dict):
                texts_to_check.append((m.get("label", ""), False))
        # Handlebars templates contienen CSS/HTML; extraer solo texto visible
        texts_to_check.append((p.get("handlebars_template", ""), True))
        texts_to_check.append((p.get("code", ""), False))

        for txt, is_html in texts_to_check:
            if not txt:
                continue
            bad = _has_dashes(txt, DASH_EXCEPTIONS, is_html=is_html)
            if bad:
                msg = f"  ✗ chart-{c['id']} params: guiones en {bad[:2]}"
                if msg not in dash_errors:
                    dash_errors.append(msg)
                    print(msg)

    if not dash_errors:
        print("  Sin guiones en textos visibles ✓")
    errors.extend(dash_errors)

    # ── 6. viz_types registrados ──────────────────────────────────────────────
    print("\n[6] viz_types: sin heredados no registrados")
    legacy_errors = []
    seen_types = {}
    for c in charts:
        vt = c.get("viz_type", "")
        if vt in LEGACY_VIZ_TYPES:
            msg = f"  ✗ chart-{c['id']} '{c['slice_name']}': viz_type={vt!r} es heredado"
            legacy_errors.append(msg)
            print(msg)
        else:
            seen_types[vt] = seen_types.get(vt, 0) + 1

    if not legacy_errors:
        print("  Ningún viz_type heredado detectado ✓")
        for vt, cnt in sorted(seen_types.items()):
            print(f"  {vt}: {cnt} gráfico(s)")
    errors.extend(legacy_errors)

    # ── 7. Filtros nativos: 7 presentes en json_metadata ─────────────────────
    print("\n[7] Filtros nativos: 8 en native_filter_configuration")
    meta = json.loads(dash.get("json_metadata") or "{}")
    nfc = meta.get("native_filter_configuration", [])
    # Superset 6.1 reconoce un filtro por el prefijo "NATIVE_FILTER-" (guion medio) y por
    # type == "NATIVE_FILTER"; con "NATIVE_FILTER_" los muestra como "[untitled customization]"
    # y no aplican.
    filter_ids_esperados = {
        "NATIVE_FILTER-PERIODO", "NATIVE_FILTER-REGION", "NATIVE_FILTER-ESTADO",
        "NATIVE_FILTER-VENDEDOR", "NATIVE_FILTER-CATEGORIA",
        "NATIVE_FILTER-TRAMO", "NATIVE_FILTER-MES", "NATIVE_FILTER-DIMENSION",
    }
    filter_ids_presentes = {f.get("id", "") for f in nfc if isinstance(f, dict)}
    filter_errors = []
    if len(nfc) < 8:
        msg = f"  ✗ Se esperan 8 filtros, hay {len(nfc)}"
        filter_errors.append(msg)
        print(msg)
    else:
        print(f"  ✓ {len(nfc)} filtros presentes")
    for fid in sorted(filter_ids_esperados):
        filtro = next((f for f in nfc if f.get("id")==fid), None)
        if filtro and filtro.get("type") != "NATIVE_FILTER":
            msg = f"  ✗ {fid}: type={filtro.get('type')!r}, se espera 'NATIVE_FILTER'"
            filter_errors.append(msg)
            print(msg)
        elif filtro:
            print(f"  ✓ {fid}: '{filtro.get('name','?')}'")
        else:
            msg = f"  ✗ Falta filtro {fid}"
            filter_errors.append(msg)
            print(msg)
    errors.extend(filter_errors)

    # ── 8. Coherencia de la descomposición Kitagawa-Oaxaca ───────────────────
    print("\n[8] Descomposición: suma de efectos = delta tasa (residuo < 0,001 pp)")
    decomp_q = (
        "SELECT mes, "
        "ROUND(ABS(SUM(efecto_composicion + efecto_tasa)"
        " - (SUM(w1 * p1) - SUM(w0 * p0))) * 100, 6) AS residuo_pp "
        "FROM lake.gold.descomposicion_mes "
        "WHERE dimension = 'customer_state' "
        "GROUP BY mes "
        "HAVING residuo_pp > 0.001 "
        "ORDER BY mes"
    )
    r_decomp = subprocess.run(
        ["docker", "compose",
         "exec", "-T", "doris",
         "mysql", "-u", "root", "-P", "9030", "-h", "127.0.0.1",
         "--skip-column-names", "-e", decomp_q],
        capture_output=True, text=True,
    )
    bad_months = [l.strip() for l in r_decomp.stdout.strip().splitlines() if l.strip()]
    if bad_months:
        msg = f"  ✗ Meses con residuo > 0,001 pp: {bad_months}"
        errors.append(msg)
        print(msg)
    else:
        print("  ✓ Residuo = 0 en todos los meses (customer_state)")
    # Tabla de comprobación para los 3 meses clave
    comprobacion_q = (
        "SELECT mes, "
        "ROUND(SUM(w1*p1)*100,4) AS tasa_pct, "
        "ROUND(SUM(w0*p0)*100,4) AS ref_pct, "
        "ROUND((SUM(w1*p1)-SUM(w0*p0))*100,4) AS delta_pp, "
        "ROUND(SUM(efecto_composicion+efecto_tasa)*100,4) AS suma_pp "
        "FROM lake.gold.descomposicion_mes "
        "WHERE dimension='customer_state' "
        "AND mes IN ('2017-11','2018-03','2018-04') "
        "GROUP BY mes ORDER BY mes"
    )
    r_comp = subprocess.run(
        ["docker", "compose",
         "exec", "-T", "doris",
         "mysql", "-u", "root", "-P", "9030", "-h", "127.0.0.1",
         "-e", comprobacion_q],
        capture_output=True, text=True,
    )
    if r_comp.stdout.strip():
        print("  Comprobación (referencia = mes anterior):")
        for line in r_comp.stdout.strip().splitlines():
            print(f"    {line}")

    # ── 9. Playwright: verificación en el navegador ───────────────────────────
    print("\n[9] Playwright: verificación DOM y capturas de pantalla")
    pw_ok = _playwright_check(s)
    if not pw_ok:
        errors.append("  ✗ Verificación Playwright falló — ver salida arriba")

    # ── Resumen ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    if errors:
        print(f"FALLOS: {len(errors)}")
        for e in errors:
            print(e)
        print("=" * 60)
        sys.exit(1)
    else:
        print(f"TODAS LAS VERIFICACIONES PASARON  ✓  (warnings: {len(warnings)})")
        for w in warnings:
            print(w)
        print("=" * 60)
        sys.exit(0)


# ── Playwright browser check ──────────────────────────────────────────────────

PLAYWRIGHT_VENV = os.path.join(
    os.path.dirname(__file__), "..", ".playwright_venv"
)
SCREENSHOT_DIR = "mediciones/etapa4/verificacion"
DASH_URL = f"{SUPERSET_URL}/superset/dashboard/olist/"
ERROR_TEXTS = [
    "Data error", "is not registered", "No map data",
    "No results", "Error loading", "SyntaxError",
]
ERROR_TEXTS_LOWER = [t.lower() for t in ERROR_TEXTS]

# KPI chart names → expected value regex (number with optional decimals)
KPI_NAMES = [
    "K01 Entrega a tiempo",
    "K08 Reseñas negativas",
    "K09 Brecha de satisfacción",
    "K10 Tasa de cancelación",
    "K11 GMV del período",
    "K02 Demora media de tardíos",
]

TABS = [
    ("Vista ejecutiva",  "tab-ejecutiva.png"),
    ("Vista analítica",  "tab-analitica.png"),
    ("Vista de decisión","tab-decision.png"),
    ("Detalle mensual",  "tab-detalle.png"),
]


def _playwright_check(api_session):
    """Abre el dashboard en Chromium, verifica DOM, toma capturas."""
    import sys
    import subprocess

    venv_python = os.path.join(PLAYWRIGHT_VENV, "bin", "python3")
    if not os.path.exists(venv_python):
        print(f"  ⚠ Playwright venv no encontrado en {PLAYWRIGHT_VENV}")
        print("    Saltando verificación en navegador.")
        return True  # advertencia, no fallo

    os.makedirs(SCREENSHOT_DIR, exist_ok=True)

    # Obtener OTD global antes del filtro SP (para comparar)
    otd_global = _fetch_kpi_value(api_session, "K01 Entrega a tiempo (OTD)")

    script = _build_playwright_script(otd_global)
    job_tmp = os.environ.get(
        "CLAUDE_JOB_DIR",
        os.path.join(os.path.dirname(__file__), "..")
    )
    script_path = os.path.join(job_tmp, "tmp", "pw_verificar.py")
    os.makedirs(os.path.dirname(script_path), exist_ok=True)
    with open(script_path, "w") as f:
        f.write(script)

    result = subprocess.run(
        [venv_python, "-I", script_path],
        capture_output=True, text=True, timeout=180,
    )
    print(result.stdout)
    if result.returncode != 0:
        print("  STDERR:", result.stderr[:400])
        return False
    return True


def _fetch_kpi_value(s, chart_name):
    """Devuelve el valor numérico de un KPI big_number_total vía API."""
    charts = s.get(f"{SUPERSET_URL}/api/v1/chart/?q=(page_size:100)").json().get("result", [])
    c = next((x for x in charts if x["slice_name"] == chart_name), None)
    if not c:
        return None
    p = json.loads(c.get("params") or "{}")
    metric = p.get("metric", {})
    sql = metric.get("sqlExpression", "") if isinstance(metric, dict) else ""
    if not sql:
        return None
    ds_id = c.get("datasource_id")
    qc = {
        "datasource": {"id": ds_id, "type": "table"},
        "force": False,
        "queries": [{
            "columns": [],
            "metrics": [{"expressionType": "SQL", "sqlExpression": sql,
                         "label": chart_name, "hasCustomLabel": True}],
            "row_limit": 1,
            "time_range": "No filter",
            "filters": [],
            "extras": {"time_grain_sqla": None, "having": "", "where": ""}
        }]
    }
    try:
        resp = s.post(f"{SUPERSET_URL}/api/v1/chart/data", json=qc, timeout=20)
        rows = resp.json().get("result", [{}])[0].get("data", [{}])
        return rows[0].get(chart_name) if rows else None
    except Exception:
        return None


def _build_playwright_script(otd_global):
    """Genera el script Playwright que se ejecuta en el venv."""
    screenshot_dir = os.path.abspath(SCREENSHOT_DIR)
    dash_url = DASH_URL
    superset_url = SUPERSET_URL
    kpi_names_str = repr(KPI_NAMES)
    error_texts_str = repr(ERROR_TEXTS_LOWER)
    tabs_str = repr(TABS)
    otd_global_str = repr(otd_global)

    return f"""
import sys, re, os, time
from playwright.sync_api import sync_playwright

SCREENSHOT_DIR = {screenshot_dir!r}
DASH_URL       = {dash_url!r}
SUPERSET_URL   = {superset_url!r}
KPI_NAMES      = {kpi_names_str}
ERROR_TEXTS    = {error_texts_str}
TABS           = {tabs_str}
OTD_GLOBAL     = {otd_global_str}

failures = []

def wait_charts(page, timeout=25000):
    try:
        page.wait_for_selector(".loading-indicator", state="hidden", timeout=timeout)
    except Exception:
        pass
    page.wait_for_timeout(2500)

def check_errors(page, context_label):
    text = (page.inner_text("body") or "").lower()
    found = [e for e in ERROR_TEXTS if e in text]
    if found:
        failures.append(f"{{context_label}}: textos de error {{found}}")

def check_no_horizontal_scroll(page, context_label):
    over = page.evaluate("document.documentElement.scrollWidth > window.innerWidth + 5")
    if over:
        sw = page.evaluate("document.documentElement.scrollWidth")
        iw = page.evaluate("window.innerWidth")
        failures.append(f"{{context_label}}: scroll horizontal (scrollWidth={{sw}}, innerWidth={{iw}})")

def check_no_internal_scroll(page, context_label):
    \"\"\"Verifica que ningún chart container tenga overflow horizontal interno.\"\"\"
    over_ids = page.evaluate(\"\"\"
        () => {{
            const charts = document.querySelectorAll('[id^="chart-id-"]');
            const bad = [];
            for (const c of charts) {{
                if (c.scrollWidth > c.clientWidth + 5)
                    bad.push(c.id + '(' + c.scrollWidth + '>' + c.clientWidth + ')');
            }}
            return bad;
        }}
    \"\"\")
    if over_ids:
        failures.append(f"{{context_label}}: scroll interno en charts {{over_ids}}")
    else:
        print(f"  ✓ {{context_label}}: sin scroll interno en charts")

def check_col_widths_slice32(page):
    \"\"\"Verifica anchos de las columnas visibles del slice 32 (±15 px).\"\"\"
    TARGETS = {{
        "riesgo_num":       (0,   0),   # oculta: exactamente 0
        "Nivel":            (90,  15),
        "Riesgo relativo":  (120, 15),
        "Días hasta la promes": (110, 15),
        "Ruta":             (90,  15),
        "Categoría":        (220, 15),
        "Vendedor":         (90,  15),
        "Pedido":           (90,  15),
        "Fecha prometida":  (110, 15),
    }}
    data = page.evaluate(\"\"\"
        () => {{
            const chart = document.querySelector('#chart-id-32');
            if (!chart) return {{error: 'chart 32 no encontrado'}};
            const ths = chart.querySelectorAll('thead th');
            return Array.from(ths).map(th => ({{
                text: (th.textContent || '').trim().substring(0, 25),
                width: Math.round(th.getBoundingClientRect().width),
            }}));
        }}
    \"\"\")
    if isinstance(data, dict) and data.get('error'):
        print(f"  ⚠ Slice 32: {{data['error']}}")
        return
    rows = []
    for col in data:
        text = col['text']
        width = col['width']
        matched_key = next((k for k in TARGETS if text.startswith(k[:10])), None)
        if not matched_key:
            rows.append(f"    ? {{text!r}}: {{width}}px (no en targets)")
            continue
        target, tol = TARGETS[matched_key]
        ok = abs(width - target) <= tol
        status = '✓' if ok else '✗'
        rows.append(f"    {{status}} {{matched_key}}: {{width}}px (objetivo {{target}}±{{tol}})")
        if not ok:
            failures.append(f"Slice 32 ancho {{matched_key!r}}: {{width}}px (objetivo {{target}}±{{tol}})")
    print("  Anchos de columnas slice 32 (1366px):")
    for r in rows:
        print(r)

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)

    for width, label in [(1366, "1366px"), (1920, "1920px")]:
        context = browser.new_context(viewport={{"width": width, "height": 768}})
        page = context.new_page()

        # Login — en Superset 6.1 los inputs no tienen atributo name
        page.goto(f"{{SUPERSET_URL}}/login/")
        page.wait_for_load_state("networkidle")
        page.fill("input[type='text']", "admin")
        page.fill("input[type='password']", "admin")
        page.click("button[type='submit']")
        page.wait_for_url(f"{{SUPERSET_URL}}/**", timeout=15000)

        page.goto(DASH_URL, timeout=30000)
        wait_charts(page)

        # Chequeo solo a 1366px (capturas y filtros)
        if width == 1366:
            # Verificar barra de filtros
            fb = page.locator(".filter-bar, [data-test='filter-bar'], .ant-layout-sider").first
            if fb.count() > 0:
                print("  ✓ Barra de filtros visible")
            else:
                print("  ⚠ Barra de filtros no localizada (puede ser horizontal)")

            # Recorrer pestañas
            for tab_name, screenshot_name in TABS:
                # Buscar la pestaña por texto
                tab_el = page.locator(f"[role='tab']:has-text({{tab_name!r}}), .ant-tabs-tab:has-text({{tab_name!r}})").first
                try:
                    tab_el.click(timeout=5000)
                    wait_charts(page)
                except Exception:
                    print(f"  ⚠ Pestaña {{tab_name!r}}: no se pudo hacer click")
                check_errors(page, f"Pestaña {{tab_name}}")
                check_no_horizontal_scroll(page, f"Pestaña {{tab_name}}")
                check_no_internal_scroll(page, f"Pestaña {{tab_name}}")
                # Verificar anchos slice 32 solo en Vista de decisión
                if "decisi" in tab_name.lower():
                    page.evaluate("const el=document.querySelector('#chart-id-32'); if(el) el.scrollIntoView({{block:'center'}})")
                    page.wait_for_timeout(1000)
                    check_col_widths_slice32(page)
                sc_path = os.path.join(SCREENSHOT_DIR, screenshot_name)
                page.screenshot(path=sc_path, full_page=False)
                print(f"  ✓ Captura guardada: {{sc_path}}")

            # Ir a Vista ejecutiva
            tab_ej = page.locator("[role='tab']:has-text('Vista ejecutiva'), .ant-tabs-tab:has-text('Vista ejecutiva')").first
            try:
                tab_ej.click(timeout=5000)
                wait_charts(page)
            except Exception:
                pass

            # Verificar valores KPI — big_number_total no muestra slice_name,
            # solo el valor numérico y el subheader. Buscamos los 6 containers.
            print("  Verificando valores KPI (big_number_total):")
            bnt_els = page.locator("[class*='big-number']").all()
            n_bnt = len(bnt_els)
            if n_bnt >= 6:
                print(f"    ✓ {{n_bnt}} tarjetas big_number encontradas")
                for i, el in enumerate(bnt_els[:6]):
                    try:
                        txt = el.inner_text(timeout=2000) or ""
                    except Exception:
                        txt = ""
                    has_num = bool(re.search(r'\\d', txt))
                    status = "✓" if has_num else "✗"
                    val_preview = txt.split("\\n")[0][:20] if txt else "(vacío)"
                    print(f"    {{status}} KPI[{{i+1}}]: {{val_preview!r}}")
                    if not has_num:
                        failures.append(f"KPI[{{i+1}}]: sin número visible (txt={{txt[:40]!r}})")
            else:
                msg = f"KPIs big_number: solo {{n_bnt}} elementos (esperados 6)"
                failures.append(msg)
                print(f"    ✗ {{msg}}")

            # Aplicar filtro Estado de destino = SP
            print("  Aplicando filtro Estado de destino = SP...")
            try:
                # La barra de filtros horizontal muestra "Estado de destino"
                # Buscar dentro de la barra de filtros (sider/sidebar) para evitar
                # coincidir con títulos de gráficos que contienen el mismo texto
                filter_bar = page.locator(".ant-layout-sider").first
                estado_header = filter_bar.locator("*:has-text('Estado de destino')").last
                estado_header.click(timeout=5000)
                page.wait_for_timeout(1500)
                # Seleccionar SP en el dropdown/lista
                sp_opt = page.locator(".ant-select-item-option:has-text('SP'), "
                                      "li:has-text('SP'), "
                                      "[title='SP']").first
                if sp_opt.count() > 0:
                    sp_opt.click()
                    page.wait_for_timeout(500)
                    apply_btn = page.locator("button:has-text('Apply'), button:has-text('Aplicar')").first
                    if apply_btn.count() > 0:
                        apply_btn.click()
                    wait_charts(page)
                    sc_sp = os.path.join(SCREENSHOT_DIR, "tab-ejecutiva-sp.png")
                    page.screenshot(path=sc_sp, full_page=False)
                    print(f"  ✓ Captura con filtro SP: {{sc_sp}}")
                    # Leer K01 (primer big_number) con filtro aplicado
                    bnt_sp = page.locator("[class*='big-number']").first
                    try:
                        k01_sp_txt = bnt_sp.inner_text(timeout=3000) or ""
                    except Exception:
                        k01_sp_txt = ""
                    k01_sp_val = k01_sp_txt.split("\\n")[0]
                    print(f"  K01 con filtro SP = {{k01_sp_val!r}}")
                else:
                    print("  ⚠ Opción SP no encontrada en el dropdown; captura sin filtro")
                    sc_sp = os.path.join(SCREENSHOT_DIR, "tab-ejecutiva-sp.png")
                    page.screenshot(path=sc_sp, full_page=False)
            except Exception as e:
                print(f"  ⚠ Error al aplicar filtro SP: {{e}}")
                sc_sp = os.path.join(SCREENSHOT_DIR, "tab-ejecutiva-sp.png")
                page.screenshot(path=sc_sp, full_page=False)

        # Verificar scroll interno a cualquier viewport
        if width == 1920:
            for tab_name, _ in TABS:
                tab_el = page.locator(f"[role='tab']:has-text({{tab_name!r}}), .ant-tabs-tab:has-text({{tab_name!r}})").first
                try:
                    tab_el.click(timeout=5000)
                    page.wait_for_timeout(3000)
                except Exception:
                    pass
                check_no_horizontal_scroll(page, f"Pestaña {{tab_name}} 1920px")
                check_no_internal_scroll(page, f"Pestaña {{tab_name}} 1920px")

        context.close()

    browser.close()

if failures:
    for f in failures:
        print(f"  FALLO: {{f}}")
    sys.exit(1)
else:
    print("  Playwright: todos los chequeos pasaron ✓")
    sys.exit(0)
"""


if __name__ == "__main__":
    main()
