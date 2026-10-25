"""Transforma silver en tablas Iceberg en lake.gold."""
import math
import numpy as np
from pyspark.sql import Window
import pyspark.sql.functions as F

from src.pipeline.config import get_spark, PERIODO_INICIO, PERIODO_FIN, N_MIN_GRUPO


def _write(df, table):
    (df.writeTo(table)
       .tableProperty("write.format.default", "parquet")
       .createOrReplace())
    print(f"  {table}: {df.count():,} filas")


def _uf_iso(col):
    return F.concat(F.lit("BR-"), F.col(col))


def main():
    spark = get_spark("olist-gold")
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")

    print("Leyendo silver...")
    pedidos = spark.table("lake.silver.pedidos")
    envios  = spark.table("lake.silver.envios")

    pedidos_util = pedidos.filter(
        (F.col("mes_compra") >= PERIODO_INICIO) & (F.col("mes_compra") <= PERIODO_FIN)
    )
    envios_util = envios.join(
        pedidos_util.select("order_id", "mes_compra").distinct(), "order_id", "inner"
    )

    # ── gold.pedidos ──────────────────────────────────────────────────────────
    print("\n[gold.pedidos]")
    gold_pedidos = (
        pedidos
        .withColumn("uf_iso", _uf_iso("customer_state"))
        .withColumn("anio_mes", F.date_format("order_purchase_timestamp", "yyyy-MM"))
    )
    _write(gold_pedidos, "lake.gold.pedidos")

    print("\n[gold.envios]")
    gold_envios = (
        envios
        .join(pedidos.select("order_id", "order_purchase_timestamp"), "order_id", "left")
        .withColumn("uf_iso_origen",  _uf_iso("seller_state"))
        .withColumn("uf_iso_destino", _uf_iso("customer_state"))
        .withColumn("anio_mes", F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .drop("order_purchase_timestamp")
    )
    _write(gold_envios, "lake.gold.envios")

    # ── gold.clientes ─────────────────────────────────────────────────────────
    print("\n[gold.clientes]")
    gold_clientes = (
        pedidos
        .groupBy("customer_unique_id")
        .agg(
            F.count("order_id").alias("n_pedidos"),
            F.min("order_purchase_timestamp").alias("primer_pedido"),
            F.max("order_purchase_timestamp").alias("ultimo_pedido"),
            F.collect_set("customer_state").alias("estados_visitados"),
        )
        .withColumn("tiene_recompra", F.col("n_pedidos") >= 2)
    )
    _write(gold_clientes, "lake.gold.clientes")

    # ── gold.vendedores ───────────────────────────────────────────────────────
    print("\n[gold.vendedores]")
    n_global_row = pedidos_util.filter(
        F.col("order_status") == "delivered"
    ).agg(
        F.count("*").alias("n"),
        F.sum(F.col("tarde").cast("int")).alias("tardios"),
    ).collect()[0]
    n_global = int(n_global_row["n"])
    p0_global = float(n_global_row["tardios"]) / n_global if n_global > 0 else 0.0
    Z95, Z998 = 1.96, 3.09

    vendor_stats = (
        envios_util
        .filter(F.col("order_status") == "delivered")
        .groupBy("seller_id", "seller_state", "seller_region")
        .agg(
            F.count("*").alias("n_envios"),
            F.sum(F.col("tarde").cast("int")).alias("n_tardios"),
            F.sum(F.col("order_status").isin("canceled", "unavailable").cast("int")).alias("n_cancelados"),
            F.avg(F.col("review_negativa").cast("int")).alias("pct_resenas_negativas"),
            F.avg(F.col("despacho_fuera_plazo").cast("int") if "despacho_fuera_plazo" in envios.columns
                  else F.lit(None)).alias("pct_despacho_fuera_plazo"),
        )
        .withColumn("tasa_tardios", F.col("n_tardios").cast("double") / F.col("n_envios"))
        .withColumn("tasa_cancelacion",
            F.col("n_cancelados").cast("double") / F.col("n_envios"))
    )

    gold_vendedores = (
        vendor_stats
        .withColumn("p0_global", F.lit(p0_global))
        .withColumn("limite_95_lo",
            F.lit(p0_global) - F.lit(Z95) * F.sqrt(F.lit(p0_global * (1 - p0_global)) / F.col("n_envios")))
        .withColumn("limite_95_hi",
            F.lit(p0_global) + F.lit(Z95) * F.sqrt(F.lit(p0_global * (1 - p0_global)) / F.col("n_envios")))
        .withColumn("limite_998_hi",
            F.lit(p0_global) + F.lit(Z998) * F.sqrt(F.lit(p0_global * (1 - p0_global)) / F.col("n_envios")))
        .withColumn("clasificacion_embudo",
            F.when(F.col("tasa_tardios") > F.col("limite_998_hi"), "arriba_998")
             .when(F.col("tasa_tardios") > F.col("limite_95_hi"),  "arriba_95")
             .when(F.col("tasa_tardios") < F.col("limite_95_lo"),  "abajo_95")
             .otherwise("dentro"))
        .withColumn("rank_tardios",
            F.rank().over(Window.orderBy(F.desc("n_tardios"))))
        .withColumn("pct_acum_vendedores",
            F.row_number().over(Window.orderBy(F.desc("n_tardios"))).cast("double")
            / F.lit(float(vendor_stats.count())))
        .withColumn("pct_acum_tardios",
            F.sum("n_tardios").over(
                Window.orderBy(F.desc("n_tardios")).rowsBetween(Window.unboundedPreceding, 0)
            ).cast("double") / F.lit(float(n_global_row["tardios"])))
    )
    _write(gold_vendedores, "lake.gold.vendedores")

    # ── gold.ciudades_mapa ────────────────────────────────────────────────────
    print("\n[gold.ciudades_mapa]")
    gold_ciudades = (
        pedidos_util
        .filter(F.col("order_status") == "delivered")
        .groupBy("ciudad_uf_cliente", "customer_lat", "customer_lng")
        .agg(
            F.count("*").alias("n_pedidos"),
            F.sum(F.col("tarde").cast("int")).alias("n_tardios"),
        )
        .withColumn("tasa_tardios", F.col("n_tardios").cast("double") / F.col("n_pedidos"))
        .filter(F.col("n_pedidos") >= N_MIN_GRUPO)
    )
    _write(gold_ciudades, "lake.gold.ciudades_mapa")

    # ── gold.descomposicion_mes ───────────────────────────────────────────────
    print("\n[gold.descomposicion_mes]")
    dimensiones = [
        ("customer_state", pedidos_util),
        ("seller_state",   envios_util.filter(F.col("order_status") == "delivered")),
        ("tramo_distancia", envios_util.filter(
            (F.col("order_status") == "delivered") & (F.col("distancia_calidad") == "ok")
        )),
    ]

    descomp_rows = []
    meses = [r[0] for r in pedidos_util.select("mes_compra").distinct().orderBy("mes_compra").collect()]

    for dim_col, base_df in dimensiones:
        if dim_col not in base_df.columns:
            continue
        for idx, mes in enumerate(meses):
            if idx < 3:
                continue
            mes_ant_3 = meses[idx - 3]
            df_ref = base_df.filter(
                (F.col("mes_compra") >= mes_ant_3) & (F.col("mes_compra") < mes)
            )
            df_cur = base_df.filter(F.col("mes_compra") == mes)
            n_ref = df_ref.count()
            n_cur = df_cur.count()
            if n_ref == 0 or n_cur == 0:
                continue

            ref_agg = (
                df_ref.groupBy(dim_col)
                .agg(
                    F.count("*").alias("n0"),
                    F.sum(F.col("tarde").cast("int")).alias("k0"),
                )
                .withColumn("w0", F.col("n0").cast("double") / n_ref)
                .withColumn("p0", F.col("k0").cast("double") / F.col("n0"))
            )
            cur_agg = (
                df_cur.groupBy(dim_col)
                .agg(
                    F.count("*").alias("n1"),
                    F.sum(F.col("tarde").cast("int")).alias("k1"),
                )
                .withColumn("w1", F.col("n1").cast("double") / n_cur)
                .withColumn("p1", F.col("k1").cast("double") / F.col("n1"))
            )
            joined = ref_agg.join(cur_agg, dim_col, "outer").fillna(0)
            rows = (
                joined
                .withColumn("mes", F.lit(mes))
                .withColumn("dimension", F.lit(dim_col))
                .withColumn("grupo", F.col(dim_col))
                .withColumn("efecto_composicion", (F.col("w1") - F.col("w0")) * F.col("p0"))
                .withColumn("efecto_tasa",         F.col("w1") * (F.col("p1") - F.col("p0")))
                .select("mes", "dimension", "grupo", "w0", "p0", "w1", "p1",
                        "efecto_composicion", "efecto_tasa")
            )
            descomp_rows.append(rows)

    if descomp_rows:
        from functools import reduce
        descomp = reduce(lambda a, b: a.union(b), descomp_rows)
        _write(descomp, "lake.gold.descomposicion_mes")

    # ── gold.promesa_ruta ─────────────────────────────────────────────────────
    print("\n[gold.promesa_ruta]")
    gold_promesa = (
        envios_util
        .filter(F.col("order_status") == "delivered")
        .join(
            pedidos_util.select("order_id", "dias_prometidos", "holgura_promesa"),
            "order_id", "left",
        )
        .groupBy("ruta_estado")
        .agg(
            F.count("*").alias("n_envios"),
            F.percentile_approx("dias_prometidos", 0.5).alias("mediana_dias_prometidos"),
            F.percentile_approx("holgura_promesa", 0.5).alias("mediana_holgura"),
            F.avg(F.col("tarde").cast("int")).alias("otd_inverso"),
        )
        .withColumn("otd", F.lit(1.0) - F.col("otd_inverso"))
        .withColumn("dias_recomendados_p90", F.lit(None).cast("double"))
        .withColumn("dias_recomendados_p95", F.lit(None).cast("double"))
        .filter(F.col("n_envios") >= N_MIN_GRUPO)
        .drop("otd_inverso")
    )
    _write(gold_promesa, "lake.gold.promesa_ruta")

    # ── gold.embudo_limites ───────────────────────────────────────────────────
    print("\n[gold.embudo_limites]")
    max_n = vendor_stats.agg(F.max("n_envios")).collect()[0][0] or 1000
    n_grid = [int(x) for x in np.logspace(0, math.log10(max_n * 1.1), 300)]
    embudo_rows = [
        (
            n,
            float(p0_global - Z95  * (p0_global * (1 - p0_global) / n) ** 0.5),
            float(p0_global + Z95  * (p0_global * (1 - p0_global) / n) ** 0.5),
            float(p0_global - Z998 * (p0_global * (1 - p0_global) / n) ** 0.5),
            float(p0_global + Z998 * (p0_global * (1 - p0_global) / n) ** 0.5),
        )
        for n in n_grid
    ]
    gold_embudo = spark.createDataFrame(
        embudo_rows,
        ["n", "limite_inf_95", "limite_sup_95", "limite_inf_998", "limite_sup_998"],
    )
    _write(gold_embudo, "lake.gold.embudo_limites")

    # ── gold.eventos ──────────────────────────────────────────────────────────
    print("\n[gold.eventos]")
    eventos_data = [
        ("2017-11-24", "Black Friday 2017",
         "Pico de pedidos Black Friday con posible saturacion logistica",
         "calendario comercial Brasil 2017", True),
        ("2018-05-21", "Paro de camioneros",
         "Huelga nacional de camioneros con bloqueos de rutas y demoras de transito",
         "noticias mayo 2018 Brasil", True),
    ]
    gold_eventos = spark.createDataFrame(
        eventos_data, ["fecha", "nombre", "descripcion", "fuente", "es_hipotesis"]
    ).withColumn("fecha", F.to_date("fecha"))
    _write(gold_eventos, "lake.gold.eventos")

    # ── gold.calidad_hallazgos ────────────────────────────────────────────────
    print("\n[gold.calidad_hallazgos]")
    hallazgos_data = [
        ("geolocation", "Puntos fuera del bounding box de Brasil",
         42, None, "filtrados en geo_cep"),
        ("geolocation", "Prefijos con estado conflictivo (8 filas disidentes)",
         8, None, "excluidos del centroide"),
        ("order_reviews", "Pedidos con mas de una resena",
         None, None, "se retiene la ultima respondida"),
        ("order_reviews", "Resenas respondidas antes de la entrega",
         4653, None, "flag resena_antes_entrega"),
        ("customers", "Prefijos postales sin geolocalizacion en geo_cep",
         None, None, "distancia_calidad=sin_geo"),
        ("sellers", "Prefijos postales sin geolocalizacion en geo_cep",
         None, None, "distancia_calidad=sin_geo"),
        ("order_items", "Precios extremos (> Q3 + 3*IQR)",
         None, None, "flag_precio_extremo"),
    ]
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
    schema_calidad = StructType([
        StructField("tabla_origen",  StringType(), True),
        StructField("descripcion",   StringType(), True),
        StructField("n_afectados",   LongType(),   True),
        StructField("pct_afectado",  DoubleType(), True),
        StructField("accion",        StringType(), True),
    ])
    gold_calidad = spark.createDataFrame(hallazgos_data, schema_calidad)
    _write(gold_calidad, "lake.gold.calidad_hallazgos")

    spark.stop()
    print("\nGold completado.")


if __name__ == "__main__":
    main()
