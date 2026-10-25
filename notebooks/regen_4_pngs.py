"""
regen_4_pngs.py — regenera los 4 PNGs que nbconvert saltó:
  S1_4b_dosis_respuesta.png   (tickangle)
  S2_1_status_dist.png        (título)
  S2_2_canc_mensual.png       (título + tickangle)
  S4_3_spearman_heatmap.png   (título)
"""
import os
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

# ─── SparkSession ──────────────────────────────────────────────────────────────
RUSTFS_ENDPOINT = os.environ.get('RUSTFS_ENDPOINT', 'http://rustfs:9000')
S3_KEY  = os.environ.get('AWS_ACCESS_KEY_ID',     'rustfsadmin')
S3_SEC  = os.environ.get('AWS_SECRET_ACCESS_KEY', 'rustfsadmin123')

JARS = ','.join([
    '/opt/spark/extra-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar',
    '/opt/spark/extra-jars/hadoop-aws-3.4.2.jar',
    '/opt/spark/extra-jars/aws-sdk-bundle-v2-2.29.52.jar',
])

spark = (
    SparkSession.builder
    .appName('regen-4-pngs')
    .master('local[*]')
    .config('spark.jars', JARS)
    .config('spark.hadoop.fs.s3a.impl',                   'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.endpoint',               RUSTFS_ENDPOINT)
    .config('spark.hadoop.fs.s3a.access.key',             S3_KEY)
    .config('spark.hadoop.fs.s3a.secret.key',             S3_SEC)
    .config('spark.hadoop.fs.s3a.path.style.access',      'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('ERROR')
print(f'Spark {spark.version} listo')

# ─── Bronze reads ──────────────────────────────────────────────────────────────
BRONZE = 's3a://olist/bronze'
orders         = spark.read.parquet(f'{BRONZE}/orders')
order_items    = spark.read.parquet(f'{BRONZE}/order_items')
order_reviews  = spark.read.parquet(f'{BRONZE}/order_reviews')
customers      = spark.read.parquet(f'{BRONZE}/customers')
products       = spark.read.parquet(f'{BRONZE}/products')
print('Bronze leído')

# ─── guardar ───────────────────────────────────────────────────────────────────
import plotly.io as pio
pio.renderers.default = 'png'

GRAFICOS = '/home/notebooks/graficos'
os.makedirs(GRAFICOS, exist_ok=True)
FIG_W, FIG_H   = 900, 500
TEMPLATE       = 'plotly_white'
FONT_FAMILY    = 'IBM Plex Sans, Arial, sans-serif'

def guardar(fig, nombre):
    fig.update_layout(template=TEMPLATE, width=FIG_W, height=FIG_H,
                      font_family=FONT_FAMILY, margin=dict(l=60, r=40, t=60, b=80))
    fig.write_image(f'{GRAFICOS}/{nombre}', scale=2)
    print(f'  → {GRAFICOS}/{nombre}')

# ─── Período útil ──────────────────────────────────────────────────────────────
PERIODO_INICIO = '2017-01'
PERIODO_FIN    = '2018-08'

# ─── todos_util (para S2_1 y S2_2) ────────────────────────────────────────────
print('\nConstruyendo todos_util...')
todos_util = (
    orders
    .filter((F.date_format("order_purchase_timestamp","yyyy-MM") >= PERIODO_INICIO) &
            (F.date_format("order_purchase_timestamp","yyyy-MM") <= PERIODO_FIN))
    .cache()
)
n_todos = todos_util.count()
print(f'  todos_util: {n_todos:,}')

status_dist = (
    todos_util.groupBy("order_status")
    .agg(F.count("*").alias("count"))
    .withColumn("pct", F.col("count").cast("double") / F.lit(n_todos))
    .orderBy(F.desc("count"))
    .cache()
)

# ─── PNG S2_1: distribución de estados ────────────────────────────────────────
print('Generando S2_1...')
fig = (
    status_dist.orderBy("count")
    .plot.barh(x="pct", y="order_status",
               title="Distribución de estados de pedido. Período útil")
)
fig.update_xaxes(title_text="% de pedidos", tickformat=".1%")
fig.update_yaxes(title_text="Estado", autorange="reversed")
fig.add_annotation(
    text=f"n = {n_todos:,}  ·  período {PERIODO_INICIO}–{PERIODO_FIN}",
    xref="paper", yref="paper", x=0, y=-0.13, showarrow=False, font_size=11)
guardar(fig, "S2_1_status_dist.png")

# ─── PNG S2_2: tasa cancelación mensual ───────────────────────────────────────
print('Generando S2_2...')
todos_util_mes = (
    todos_util
    .withColumn("mes_compra", F.date_format("order_purchase_timestamp","yyyy-MM"))
    .withColumn("cancelado", F.col("order_status").isin("canceled","unavailable").cast("int"))
)
tasa_canc_mes = (
    todos_util_mes.groupBy("mes_compra")
    .agg(F.count("*").alias("n"), F.sum("cancelado").alias("cancelados"))
    .withColumn("p", F.col("cancelados").cast("double") / F.col("n"))
    .orderBy("mes_compra")
)
fig = tasa_canc_mes.plot.bar(x="mes_compra", y="p",
    title="Tasa de cancelación + no disponibilidad mensual. Período útil")
fig.update_xaxes(title_text="Mes de compra", tickangle=-45)
fig.update_yaxes(title_text="Tasa cancelación", tickformat=".1%")
fig.add_annotation(
    text=f"n={n_todos:,}  ·  período {PERIODO_INICIO}–{PERIODO_FIN}",
    xref="paper", yref="paper", x=0, y=-0.13, showarrow=False, font_size=11)
guardar(fig, "S2_2_canc_mensual.png")

# ─── Variables para S1_4b (dosis-respuesta) ────────────────────────────────────
print('\nConstruyendo pedidos_tardios...')
delivered = orders.filter(F.col("order_status") == "delivered")
pedidos_tardios = (
    delivered
    .filter(F.col("order_delivered_customer_date").isNotNull() &
            F.col("order_estimated_delivery_date").isNotNull())
    .withColumn("dias_atraso",
        F.datediff(F.to_date("order_delivered_customer_date"),
                   F.to_date("order_estimated_delivery_date")))
    .withColumn("tarde", F.col("dias_atraso") > 0)
    .withColumn("mes_compra", F.date_format("order_purchase_timestamp","yyyy-MM"))
    .cache()
)

pedidos_util = (
    pedidos_tardios
    .filter((F.col("mes_compra") >= PERIODO_INICIO) &
            (F.col("mes_compra") <= PERIODO_FIN))
    .cache()
)
n_base = pedidos_util.count()
print(f'  pedidos_util: {n_base:,}')

# resena deduplicada
print('Construyendo resena...')
w_resena = Window.partitionBy("order_id").orderBy(F.desc("review_answer_timestamp"))
resena = (
    order_reviews
    .withColumn("rk", F.row_number().over(w_resena))
    .filter(F.col("rk") == 1)
    .drop("rk")
    .withColumn("negativa", F.col("review_score") <= 2)
    .withColumn("resena_antes_entrega",
        F.col("review_answer_timestamp") < F.col("review_creation_date"))
    .cache()
)

# ped_resena
print('Construyendo ped_resena...')
ped_resena = (
    pedidos_util.select("order_id","tarde","dias_atraso")
    .join(resena.select("order_id","review_score","negativa","resena_antes_entrega"),
          "order_id", "inner")
    .cache()
)
n_pr = ped_resena.count()
print(f'  ped_resena: {n_pr:,}')

# pct_neg_global
pct_neg_global = ped_resena.filter(F.col("negativa")).count() / n_pr
print(f'  pct_neg_global: {pct_neg_global:.3f}')

# dosis-respuesta
print('Construyendo dosis...')
tramo_expr = (
    F.when(F.col("dias_atraso") <= -10, "1_≤-10")
     .when(F.col("dias_atraso") <= -5,  "2_-9 a -5")
     .when(F.col("dias_atraso") <= 0,   "3_-4 a 0")
     .when(F.col("dias_atraso") <= 3,   "4_1 a 3")
     .when(F.col("dias_atraso") <= 7,   "5_4 a 7")
     .when(F.col("dias_atraso") <= 14,  "6_8 a 14")
     .when(F.col("dias_atraso") <= 30,  "7_15 a 30")
     .otherwise(                         "8_>30")
)
Z_w = 1.96
dosis = (
    ped_resena
    .withColumn("tramo_atraso", tramo_expr)
    .groupBy("tramo_atraso")
    .agg(F.count("*").alias("n"),
         F.sum(F.col("negativa").cast("int")).alias("negativas"))
    .filter(F.col("n") >= 30)
    .withColumn("p", F.col("negativas").cast("double") / F.col("n"))
    .withColumn("wilson_lo",
        (F.col("p") + F.lit(Z_w**2)/(2*F.col("n")) -
         F.lit(Z_w)*F.sqrt(F.col("p")*(1-F.col("p"))/F.col("n") +
             F.lit(Z_w**2)/(4*F.col("n")**2))) /
        (F.lit(1.0) + F.lit(Z_w**2)/F.col("n")))
    .withColumn("wilson_hi",
        (F.col("p") + F.lit(Z_w**2)/(2*F.col("n")) +
         F.lit(Z_w)*F.sqrt(F.col("p")*(1-F.col("p"))/F.col("n") +
             F.lit(Z_w**2)/(4*F.col("n")**2))) /
        (F.lit(1.0) + F.lit(Z_w**2)/F.col("n")))
    .orderBy("tramo_atraso")
    .cache()
)
dosis.show()

# ─── PNG S1_4b: dosis-respuesta ────────────────────────────────────────────────
print('Generando S1_4b...')
rows_d   = dosis.orderBy("tramo_atraso").collect()
labels_d = [r["tramo_atraso"].split("_",1)[1] for r in rows_d]
p_d      = [r["p"] for r in rows_d]
n_d      = [r["n"] for r in rows_d]

fig = go.Figure()
fig.add_trace(go.Bar(
    x=labels_d, y=p_d, name="% negativas",
    marker_color="#2196F3",
    text=[f"n={n}" for n in n_d], textposition="outside",
))
fig.add_hline(y=pct_neg_global, line_dash="dash", line_color="#E53935",
              annotation_text=f"Global {pct_neg_global:.1%}")
fig.update_xaxes(title_text="Tramo de días de atraso", tickangle=-45)
fig.update_yaxes(title_text="% reseñas negativas (≤2)", tickformat=".0%")
fig.add_annotation(
    text=f"n={n_pr:,}  ·  período {PERIODO_INICIO}–{PERIODO_FIN}  ·  solo tramos con ≥30 pedidos",
    xref="paper", yref="paper", x=0, y=-0.18, showarrow=False, font_size=11)
guardar(fig, "S1_4b_dosis_respuesta.png")

# ─── Variables para S4_3 (Spearman) ───────────────────────────────────────────
print('\nConstruyendo features para Spearman...')
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

# products_fl con flags
products_fl = (
    products
    .withColumn("flag_peso_invalido",
        F.col("product_weight_g").isNull() | (F.col("product_weight_g") == 0))
    .withColumn("volumen_cm3",
        F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"))
)

# dias_prometidos
dias_prom_df = (
    orders.select("order_id",
        F.datediff(F.to_date("order_estimated_delivery_date"),
                   F.to_date("order_purchase_timestamp")).alias("dias_prometidos"))
)

# items_agg
items_agg = (
    order_items
    .join(products_fl.select("product_id","product_weight_g","volumen_cm3"), "product_id","left")
    .groupBy("order_id")
    .agg(
        F.sum("price").alias("precio_total"),
        F.sum("freight_value").alias("flete_total"),
        F.count("*").alias("n_items"),
        F.avg("product_weight_g").alias("peso_medio"),
        F.avg("volumen_cm3").alias("vol_medio"),
    )
)

# join con pedidos_util
features_raw = (
    pedidos_util.select("order_id","dias_atraso","tarde")
    .join(dias_prom_df, "order_id", "left")
    .join(items_agg,    "order_id", "left")
    .join(resena.select("order_id","review_score"), "order_id", "left")
    .select(
        "dias_atraso", "dias_prometidos",
        "precio_total", "flete_total", "n_items",
        "peso_medio", "vol_medio", "review_score",
    )
    .cache()
)

FEATURE_COLS = [
    "dias_atraso", "dias_prometidos",
    "precio_total", "flete_total", "n_items",
    "peso_medio", "vol_medio", "review_score",
]

assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip")
features_vect = assembler.transform(features_raw)
n_corr = features_vect.count()
print(f'  n_corr: {n_corr:,}')

corr_result = Correlation.corr(features_vect, "features", "spearman").collect()[0][0]
corr_array  = corr_result.toArray()
print("  Correlación Spearman calculada")

# ─── PNG S4_3: heatmap Spearman ────────────────────────────────────────────────
print('Generando S4_3...')
corr_pd = pd.DataFrame(corr_array, index=FEATURE_COLS, columns=FEATURE_COLS)

fig = px.imshow(
    corr_pd, text_auto=".2f",
    color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
    title="Correlaciones de Spearman. Variables numéricas a nivel pedido",
)
fig.update_layout(coloraxis_colorbar_title="Spearman ρ")
fig.add_annotation(
    text=f"n={n_corr:,} pedidos  ·  período {PERIODO_INICIO}–{PERIODO_FIN}  ·  excepción: toPandas de la matriz 8×8",
    xref="paper", yref="paper", x=0, y=-0.15, showarrow=False, font_size=11)
guardar(fig, "S4_3_spearman_heatmap.png")

print('\n=== Todos los PNGs generados ===')
spark.stop()
