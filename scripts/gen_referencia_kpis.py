"""Calcula los KPIs principales desde gold y guarda referencia_kpis.csv."""
import os
import sys
sys.path.insert(0, "/repo")

from pathlib import Path
import pyspark.sql.functions as F
from src.pipeline.config import get_spark, PERIODO_INICIO, PERIODO_FIN

OUTPUT = Path(os.environ.get("OUTPUT_PATH", "/home/notebooks/referencia_kpis.csv"))


def main():
    spark = get_spark("olist-kpis")
    spark.sparkContext.setLogLevel("ERROR")

    pedidos = spark.table("lake.gold.pedidos")
    envios  = spark.table("lake.gold.envios")
    vend    = spark.table("lake.gold.vendedores")
    cli     = spark.table("lake.gold.clientes")

    pe = pedidos.filter(
        (F.col("order_status") == "delivered")
        & (F.col("en_periodo_util") == True)
    )
    pd_activos = pedidos.filter(
        (F.col("en_periodo_util") == True)
        & (~F.col("order_status").isin("canceled", "unavailable"))
    )

    def val(df, expr, alias="v"):
        return df.selectExpr(f"{expr} AS {alias}").first()[alias]

    rows = []

    k01 = val(pe, "AVG(CASE WHEN dias_atraso <= 0 THEN 1.0 ELSE 0.0 END)")
    rows.append({"kpi": "K01_OTD", "valor": k01, "descripcion": "Tasa de entrega a tiempo",
                 "unidad": "proporcion", "poblacion": "entregados en periodo util"})

    k02_media = val(pe.filter(F.col("dias_atraso") > 0),
                    "AVG(dias_atraso)")
    rows.append({"kpi": "K02_demora_media", "valor": k02_media, "descripcion": "Demora media de tardios",
                 "unidad": "dias", "poblacion": "entregados tardios en periodo util"})

    k02_p50 = val(pe.filter(F.col("dias_atraso") > 0),
                  "APPROX_PERCENTILE(dias_atraso, 0.5)")
    rows.append({"kpi": "K02_demora_p50", "valor": k02_p50, "descripcion": "Demora mediana de tardios",
                 "unidad": "dias", "poblacion": "entregados tardios en periodo util"})

    k03_p50 = val(pe, "APPROX_PERCENTILE(t_total, 0.5)")
    k03_p90 = val(pe, "APPROX_PERCENTILE(t_total, 0.9)")
    rows.append({"kpi": "K03_leadtime_p50", "valor": k03_p50, "descripcion": "Lead time mediana",
                 "unidad": "dias", "poblacion": "entregados en periodo util"})
    rows.append({"kpi": "K03_leadtime_p90", "valor": k03_p90, "descripcion": "Lead time P90",
                 "unidad": "dias", "poblacion": "entregados en periodo util"})

    k04 = val(pe, "APPROX_PERCENTILE(t_despacho, 0.5)")
    rows.append({"kpi": "K04_despacho_p50", "valor": k04, "descripcion": "Tiempo de despacho mediana",
                 "unidad": "dias", "poblacion": "entregados en periodo util"})

    k05 = val(pe.filter(F.col("despacho_fuera_plazo").isNotNull()),
              "AVG(CASE WHEN despacho_fuera_plazo THEN 1.0 ELSE 0.0 END)")
    rows.append({"kpi": "K05_fuera_plazo", "valor": k05, "descripcion": "Incumplimiento limite despacho",
                 "unidad": "proporcion", "poblacion": "entregados con despacho_fuera_plazo registrado"})

    k06 = val(pe, "APPROX_PERCENTILE(t_transito, 0.5)")
    rows.append({"kpi": "K06_transito_p50", "valor": k06, "descripcion": "Tiempo de transito mediana",
                 "unidad": "dias", "poblacion": "entregados en periodo util"})

    k07 = val(pe, "APPROX_PERCENTILE(holgura_promesa, 0.5)")
    rows.append({"kpi": "K07_holgura_p50", "valor": k07, "descripcion": "Holgura de la promesa mediana",
                 "unidad": "dias", "poblacion": "entregados en periodo util"})

    k08_neg = val(pe.filter(F.col("review_score").isNotNull()),
                  "AVG(CASE WHEN review_negativa THEN 1.0 ELSE 0.0 END)")
    k08_score = val(pe.filter(F.col("review_score").isNotNull()), "AVG(review_score)")
    rows.append({"kpi": "K08_resenas_neg", "valor": k08_neg, "descripcion": "Proporcion resenas negativas",
                 "unidad": "proporcion", "poblacion": "entregados con review_score"})
    rows.append({"kpi": "K08_score_medio", "valor": k08_score, "descripcion": "Puntaje medio de resenas",
                 "unidad": "puntos 1-5", "poblacion": "entregados con review_score"})

    pe_sin_sesgo = pe.filter(
        F.col("review_score").isNotNull() & (F.col("resena_antes_entrega") == False)
    )
    k09 = val(pe_sin_sesgo,
              "AVG(CASE WHEN tarde AND review_negativa THEN 1.0 ELSE 0.0 END)"
              " - AVG(CASE WHEN NOT tarde AND review_negativa THEN 1.0 ELSE 0.0 END)")
    rows.append({"kpi": "K09_brecha", "valor": k09, "descripcion": "Brecha satisfaccion tardios vs puntuales",
                 "unidad": "diferencia de proporciones", "poblacion": "entregados sin resena anticipada"})

    k10 = val(pedidos.filter(F.col("en_periodo_util") == True),
              "AVG(CASE WHEN order_status IN ('canceled','unavailable') THEN 1.0 ELSE 0.0 END)")
    rows.append({"kpi": "K10_cancelacion", "valor": k10, "descripcion": "Tasa de cancelacion",
                 "unidad": "proporcion", "poblacion": "todos los pedidos del periodo util"})

    k11 = val(pd_activos, "SUM(gmv)")
    rows.append({"kpi": "K11_GMV_total", "valor": k11, "descripcion": "GMV total del periodo",
                 "unidad": "R$", "poblacion": "pedidos activos del periodo util"})

    k12 = val(pd_activos, "SUM(gmv) / NULLIF(COUNT(order_id), 0)")
    rows.append({"kpi": "K12_AOV", "valor": k12, "descripcion": "Ticket promedio",
                 "unidad": "R$", "poblacion": "pedidos activos del periodo util"})

    k13 = val(pd_activos, "SUM(flete_total) / NULLIF(SUM(precio_total), 0)")
    rows.append({"kpi": "K13_flete_ratio", "valor": k13, "descripcion": "Ratio flete sobre precio",
                 "unidad": "proporcion", "poblacion": "pedidos activos del periodo util"})

    k14 = val(vend,
              "SUM(CASE WHEN pct_acum_vendedores <= 0.1 THEN n_tardios ELSE 0 END) / NULLIF(SUM(n_tardios), 0)")
    rows.append({"kpi": "K14_concentracion", "valor": k14, "descripcion": "Concentracion tardios en top 10pct vendedores",
                 "unidad": "proporcion", "poblacion": "vendedores del periodo util"})

    k15 = val(cli, "AVG(CASE WHEN tiene_recompra THEN 1.0 ELSE 0.0 END)")
    rows.append({"kpi": "K15_recompra", "valor": k15, "descripcion": "Tasa de recompra",
                 "unidad": "proporcion", "poblacion": "todos los clientes unicos"})

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w") as f:
        f.write("kpi,valor,descripcion,unidad,poblacion\n")
        for r in rows:
            v = f"{r['valor']:.6f}" if r["valor"] is not None else ""
            desc = r["descripcion"].replace(",", ";")
            pob  = r["poblacion"].replace(",", ";")
            f.write(f"{r['kpi']},{v},{desc},{r['unidad']},{pob}\n")

    print(f"\nReferencia KPIs guardada en {OUTPUT}")
    print("\nValores:")
    for r in rows:
        v = f"{r['valor']:.4f}" if r["valor"] is not None else "N/A"
        print(f"  {r['kpi']:<30} {v}")

    spark.stop()


if __name__ == "__main__":
    main()
