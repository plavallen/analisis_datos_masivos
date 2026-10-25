"""
Parámetros globales del modelo de regresión logística para pedidos tardíos.
Sin números mágicos en el resto del código: todo se importa desde acá.
"""

# ── Catálogo Iceberg ────────────────────────────────────────────────────────
CATALOGO = "lake"
SILVER_PEDIDOS  = f"{CATALOGO}.silver.pedidos"
SILVER_ENVIOS   = f"{CATALOGO}.silver.envios"

# ── Partición temporal ──────────────────────────────────────────────────────
FECHA_FIN_TRAIN = "2017-12-31"
FECHA_FIN_VAL   = "2018-03-31"
FECHA_FIN_TEST  = "2018-08-31"

# ── Historial del vendedor ──────────────────────────────────────────────────
VENTANA_HISTORIAL_DIAS = 90
M_SUAVIZADO            = 30       # parámetro bayesiano de suavizado de tasa

# ── Margen de promesa ───────────────────────────────────────────────────────
MIN_PEDIDOS_RUTA   = 30   # mínimo para usar mediana de ruta; si <, usa estado destino
MIN_PEDIDOS_ESTADO = 30   # mínimo para usar mediana de estado; si <, usa global

# ── AgruparRaras ────────────────────────────────────────────────────────────
MIN_EVENTOS_DEFAULT = 20

# ── Búsqueda de hiperparámetros ─────────────────────────────────────────────
N_COMBINACIONES_BUSQUEDA = 30
SEMILLA                  = 42
REG_PARAM_LO             = 1e-4
REG_PARAM_HI             = 1.0
ELASTIC_NET_LO           = 0.0
ELASTIC_NET_HI           = 1.0
MIN_EVENTOS_OPCIONES     = [20, 30, 50]

# ── Umbral por costo ────────────────────────────────────────────────────────
# Costo de una intervención dividido costo de un tardío sin intervención.
# Umbral óptimo = C_intervencion / C_tarde
C_INTERVENCION = 1.0
C_TARDE        = 5.0
UMBRAL_COSTO   = C_INTERVENCION / C_TARDE

# ── Salidas ─────────────────────────────────────────────────────────────────
DIR_GRAFICOS       = "mediciones/etapa3/logistica"
DIR_MODELO         = "mediciones/etapa3/logistica/modelo"
DIR_AGRUPAMIENTOS  = "docs/modelo/agrupamientos.yaml"
NOMBRE_MODELO_MLFLOW = "olist_tarde_logistica"
ALIAS_MODELO         = "challenger"

# ── MLflow ──────────────────────────────────────────────────────────────────
# La URI se lee de la variable de entorno MLFLOW_TRACKING_URI (configurada
# en el docker-compose). Solo se sobreescribe acá si se ejecuta fuera del contenedor.
EXPERIMENTO_MLFLOW = "logistica_pedidos_tardios"
