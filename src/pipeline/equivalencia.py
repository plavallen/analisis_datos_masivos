"""
Verifica equivalencia entre las cifras del EDA (notebooks) y el pipeline.
Guarda resultado en mediciones/etapa2/eda/referencia_kpis.csv.
"""
import csv
import os
import pyspark.sql.functions as F

from src.pipeline.config import get_spark, BRONZE_BASE, PERIODO_INICIO, PERIODO_FIN
from src.pipeline.limpieza import normalizar_zip, canonicalizar_ciudad, limpiar_geolocation
from src.pipeline.geo import calcular_geo_cep
from src.pipeline.entregas import agregar_tiempos
from src.pipeline.resenas import una_resena_por_pedido, agregar_flags_resena

REFERENCIA = {
    "n_pedidos_periodo_util": 96203,
    "tasa_global_tardios":    0.0679,
    "pct_resenas_negativas":  None,
    "n_prefijos_geo_cep":     19011,
    "cobertura_distancia":    None,
}

OUT_CSV = "mediciones/etapa2/eda/referencia_kpis.csv"


def main():
    spark = get_spark("olist-equivalencia")
    spark.sparkContext.setLogLevel("ERROR")

    orders       = spark.read.parquet(f"{BRONZE_BASE}/orders")
    order_reviews = spark.read.parquet(f"{BRONZE_BASE}/order_reviews")
    geolocation  = spark.read.parquet(f"{BRONZE_BASE}/geolocation")
    customers    = spark.read.parquet(f"{BRONZE_BASE}/customers")

    print("Calculando KPIs desde bronze con módulos del pipeline...")

    geo_limpio = limpiar_geolocation(geolocation)
    geo_cep    = calcular_geo_cep(geo_limpio)
    n_prefijos = geo_cep.count()

    orders_t = agregar_tiempos(orders)
    pedidos_util = orders_t.filter(
        (F.col("mes_compra") >= PERIODO_INICIO)
        & (F.col("mes_compra") <= PERIODO_FIN)
        & (F.col("order_status") == "delivered")
    )

    row_global = pedidos_util.agg(
        F.count("*").alias("n"),
        F.avg(F.col("tarde").cast("int")).alias("tasa"),
    ).collect()[0]
    n_util       = int(row_global["n"])
    tasa_tardios = float(row_global["tasa"])

    reviews_s = una_resena_por_pedido(order_reviews)
    reviews_s = agregar_flags_resena(reviews_s, orders)
    ped_res   = pedidos_util.join(
        reviews_s.select("order_id", "review_negativa"), "order_id", "inner"
    )
    pct_neg = float(ped_res.agg(F.avg(F.col("review_negativa").cast("int"))).collect()[0][0] or 0)

    pipeline_vals = {
        "n_pedidos_periodo_util": float(n_util),
        "tasa_global_tardios":    tasa_tardios,
        "pct_resenas_negativas":  pct_neg,
        "n_prefijos_geo_cep":     float(n_prefijos),
    }

    rows = []
    for kpi, val_pipe in pipeline_vals.items():
        val_ref = REFERENCIA.get(kpi)
        if val_ref is None:
            diff = None
            ok   = None
        else:
            diff = abs(val_pipe - val_ref)
            rel  = diff / val_ref if val_ref != 0 else diff
            ok   = rel < 0.01

        print(f"  {kpi:<35} notebook={val_ref}  pipeline={val_pipe:.4f}  ok={ok}")
        rows.append({
            "kpi":            kpi,
            "valor_notebook": val_ref if val_ref is not None else val_pipe,
            "valor_pipeline": val_pipe,
            "diferencia":     diff if diff is not None else 0.0,
            "ok":             ok if ok is not None else True,
        })

    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["kpi", "valor_notebook", "valor_pipeline", "diferencia", "ok"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nReferencia guardada en {OUT_CSV}")

    spark.stop()


if __name__ == "__main__":
    main()
