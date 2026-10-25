"""
fix_plots.py — aplica correcciones a las celdas de graficado de etapa2_eda.ipynb
y re-ejecuta solo esas celdas en el kernel ya activo.

Correcciones:
  1. Eliminar "— pedidos entregados 2016–2018" y similares de los títulos
  2. Reemplazar " — " como separador con ". " en títulos
  3. Agregar tickangle=-45 en barras con eje X temporal (mes_compra)
  4. Celda 194: reemplazar boxplot por barras horizontales con IQR
"""
import json, sys

NB = '/home/notebooks/etapa2_eda.ipynb'

with open(NB) as f:
    nb = json.load(f)
cells = nb['cells']


def src(idx):
    c = cells[idx]
    return ''.join(c['source']) if isinstance(c['source'], list) else c['source']


def set_src(idx, code):
    cells[idx]['source'] = code
    # limpiar outputs anteriores
    if cells[idx]['cell_type'] == 'code':
        cells[idx]['outputs'] = []
        cells[idx]['execution_count'] = None
    print(f'  Celda {idx} → modificada')


# ─── 184: tramos — quitar subtítulo + tickangle ────────────────────────────────
s = src(184)
s = s.replace(
    'title="Distribución de envíos por tramo de distancia — pedidos entregados 2016–2018"',
    'title="Distribución de envíos por tramo de distancia"',
)
# agregar tickangle antes de guardar
s = s.replace(
    'fig.update_xaxes(title_text="Tramo")',
    'fig.update_xaxes(title_text="Tramo", tickangle=-45)',
)
set_src(184, s)

# ─── 191: rutas — quitar subtítulo ─────────────────────────────────────────────
s = src(191)
s = s.replace(
    'title="Distancia mediana top-15 rutas estado→estado — pedidos entregados 2016–2018"',
    'title="Distancia mediana. Top-15 rutas estado→estado"',
)
set_src(191, s)

# ─── 194: boxplot → barras horizontales con IQR ────────────────────────────────
set_src(194, """\
# E6c — Barras horizontales: distancia mediana por región de destino (mediana + IQR)
import plotly.graph_objects as go

rows = box_stats.orderBy("med").collect()
nombres  = [r["region_destino"] for r in rows]
medianas = [r["med"] for r in rows]
lo_err   = [r["med"] - r["q1"] for r in rows]
hi_err   = [r["q3"] - r["med"] for r in rows]

fig = go.Figure(go.Bar(
    x=medianas, y=nombres, orientation="h",
    error_x=dict(type="data", symmetric=False,
                 array=hi_err, arrayminus=lo_err, color="#555555"),
    marker_color="#2196F3",
))
fig.update_xaxes(title_text="Distancia mediana (km)", tickformat=",")
fig.update_yaxes(title_text="Región de destino", autorange="reversed")
fig.update_layout(title="Distancia de envío por región de destino")
fig.add_annotation(
    text=f"n = {n_ok:,} envíos · barras = mediana · error = IQR · cuartiles via percentile_approx · distancia_calidad = ok",
    xref="paper", yref="paper", x=0, y=-0.13, showarrow=False, font_size=11)
guardar(fig, "E6_dist_boxplot_region.png")
""")

# ─── 203: A5 histograma — quitar subtítulo ────────────────────────────────────
s = src(203)
s = s.replace(
    'title="Distribución de días de atraso — pedidos entregados con ambas fechas"',
    'title="Distribución de días de atraso (pedidos con ambas fechas)"',
)
set_src(203, s)

# ─── 205: B1 pedidos por mes — agregar tickangle ──────────────────────────────
s = src(205)
s = s.replace(
    'fig.update_xaxes(title_text="Mes de compra")',
    'fig.update_xaxes(title_text="Mes de compra", tickangle=-45)',
)
set_src(205, s)

# ─── 207: B2 tasa tardíos por mes — agregar tickangle ─────────────────────────
s = src(207)
s = s.replace(
    'fig.update_xaxes(title_text="Mes de compra")',
    'fig.update_xaxes(title_text="Mes de compra", tickangle=-45)',
)
set_src(207, s)

# ─── 241: funnel vendedores — reemplazar " — " en título ──────────────────────
s = src(241)
s = s.replace(
    'title="Gráfico de embudo — vendedores vs. tasa de tardíos (Spiegelhalter 2005)"',
    'title="Gráfico de embudo. Vendedores vs. tasa de tardíos (Spiegelhalter 2005)"',
)
set_src(241, s)

# ─── 262: dosis-respuesta — agregar tickangle ─────────────────────────────────
s = src(262)
s = s.replace(
    'fig.update_xaxes(title_text="Tramo de días de atraso")',
    'fig.update_xaxes(title_text="Tramo de días de atraso", tickangle=-45)',
)
set_src(262, s)

# ─── 271: status dist — reemplazar " — " en título ────────────────────────────
s = src(271)
s = s.replace(
    'title="Distribución de estados de pedido — período útil"',
    'title="Distribución de estados de pedido. Período útil"',
)
set_src(271, s)

# ─── 273: cancelación mensual — " — " + tickangle ─────────────────────────────
s = src(273)
s = s.replace(
    'title="Tasa de cancelación + no disponibilidad mensual — período útil"',
    'title="Tasa de cancelación + no disponibilidad mensual. Período útil"',
)
s = s.replace(
    'fig.update_xaxes(title_text="Mes de compra")',
    'fig.update_xaxes(title_text="Mes de compra", tickangle=-45)',
)
set_src(273, s)

# ─── 300: spearman — reemplazar " — " en título ───────────────────────────────
s = src(300)
s = s.replace(
    'title="Matriz de correlaciones de Spearman — variables numéricas a nivel pedido"',
    'title="Correlaciones de Spearman. Variables numéricas a nivel pedido"',
)
set_src(300, s)

# ─── Guardar notebook ──────────────────────────────────────────────────────────
with open(NB, 'w') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)
print('\nNotebook guardado.')

# ─── Re-ejecutar celdas de graficado vía kernel activo ─────────────────────────
import jupyter_client, time

CF = '/root/.local/share/jupyter/runtime/kernel-7c95adf6-3f2e-4431-bc02-21060b7bac4a.json'

kc = jupyter_client.BlockingKernelClient()
kc.load_connection_file(CF)
kc.start_channels()
try:
    kc.wait_for_ready(timeout=15)
    print('Kernel conectado.\n')
except RuntimeError as e:
    print(f'Error conectando al kernel: {e}')
    sys.exit(1)


def run_cell(idx, timeout=90):
    code = src(idx)  # leer la fuente ya modificada del notebook en memoria
    print(f'  Ejecutando celda {idx}...', end=' ', flush=True)
    kc.execute(code)
    t0 = time.time()
    errors = []
    while True:
        try:
            msg = kc.get_iopub_msg(timeout=timeout)
        except Exception:
            print(f'TIMEOUT ({time.time()-t0:.0f}s)')
            return False
        mt = msg['msg_type']
        if mt == 'status' and msg['content']['execution_state'] == 'idle':
            print(f'OK ({time.time()-t0:.1f}s)')
            return True
        elif mt == 'error':
            tb = '\n'.join(msg['content']['traceback'])
            errors.append(tb)
            print(f'ERROR')
            print(tb[:600])
            return False


# Celdas a re-ejecutar (solo las modificadas que producen PNG)
TO_EXEC = [184, 191, 194, 203, 205, 207, 241, 262, 271, 273, 300]

ok_count = 0
for idx in TO_EXEC:
    if run_cell(idx):
        ok_count += 1

kc.stop_channels()
print(f'\n{ok_count}/{len(TO_EXEC)} celdas ejecutadas correctamente.')
