"""Pruebas de calidad sobre las capas silver y gold."""
import sys
import csv
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from src.pipeline.config import get_spark, PERIODO_INICIO, PERIODO_FIN


def unicidad_claves(spark: SparkSession, tabla: str, cols_pk: list) -> DataFrame:
    """Retorna filas duplicadas en la clave primaria de la tabla."""
    df = spark.table(tabla)
    return (
        df.groupBy(*cols_pk)
        .count()
        .filter(F.col("count") > 1)
    )


def fks_huerfanas(
    spark: SparkSession,
    tabla_hijo: str, col_hijo: str,
    tabla_padre: str, col_padre: str,
) -> DataFrame:
    """Retorna filas del hijo sin correspondencia en el padre."""
    hijo  = spark.table(tabla_hijo).select(F.col(col_hijo))
    padre = spark.table(tabla_padre).select(F.col(col_padre))
    return hijo.join(padre, hijo[col_hijo] == padre[col_padre], "left_anti")


def conteos_bronze_silver(spark: SparkSession, tabla: str) -> DataFrame:
    """
    Verifica que bronze_count == silver_count + cuarentena_count.
    Retorna una fila con la diferencia; vacío si cuadra.
    """
    n_bronze = spark.read.parquet(f"s3a://olist/bronze/{tabla}").count()
    n_silver = spark.table(f"lake.silver.{tabla}").count()
    n_cuaren = (
        spark.table("lake.silver.cuarentena")
        .filter(F.col("tabla_origen") == tabla)
        .count()
    )
    diff = n_bronze - (n_silver + n_cuaren)
    if diff != 0:
        return spark.createDataFrame(
            [(tabla, n_bronze, n_silver, n_cuaren, diff)],
            ["tabla", "n_bronze", "n_silver", "n_cuarentena", "diferencia"],
        )
    return spark.createDataFrame(
        [], "tabla string, n_bronze long, n_silver long, n_cuarentena long, diferencia long"
    )


def orden_fechas(spark: SparkSession) -> DataFrame:
    """Retorna pedidos con fechas del ciclo fuera de orden sin flag marcado."""
    return (
        spark.table("lake.silver.orders")
        .filter(
            (F.col("flag_despacho_antes_aprobacion") == False)
            & (F.col("t_despacho") < 0)
        )
    )


def identidad_descomposicion(spark: SparkSession) -> DataFrame:
    """
    Verifica exceso_vendedor + exceso_transporte == dias_atraso para tardíos.
    Retorna filas donde la diferencia supera 0.01 días.
    """
    return (
        spark.table("lake.silver.pedidos")
        .filter(F.col("tarde") == True)
        .withColumn(
            "diff",
            F.abs(
                F.col("dias_atraso").cast("double")
                - F.col("exceso_vendedor")
                - F.col("exceso_transporte")
            ),
        )
        .filter(F.col("diff") > 0.01)
        .select("order_id", "dias_atraso", "exceso_vendedor", "exceso_transporte", "diff")
    )


def coherencia_eda(spark: SparkSession, referencia_csv: str) -> DataFrame:
    """
    Compara KPIs del pipeline contra cifras de referencia.
    Retorna filas donde la diferencia relativa supera 0.01.
    """
    pedidos = spark.table("lake.silver.pedidos")
    pedidos_util = pedidos.filter(
        (F.col("mes_compra") >= PERIODO_INICIO)
        & (F.col("mes_compra") <= PERIODO_FIN)
        & (F.col("order_status") == "delivered")
    )

    row = pedidos_util.agg(
        F.count("*").alias("n"),
        F.avg(F.col("tarde").cast("int")).alias("tasa_tardios"),
        F.avg(F.col("review_negativa").cast("int")).alias("pct_neg"),
    ).collect()[0]

    n_cep = spark.table("lake.silver.geo_cep").count()

    pipeline_vals = {
        "n_pedidos_periodo_util": float(row["n"]),
        "tasa_global_tardios":    float(row["tasa_tardios"]),
        "pct_resenas_negativas":  float(row["pct_neg"]) if row["pct_neg"] else 0.0,
        "n_prefijos_geo_cep":     float(n_cep),
    }

    referencia = {}
    try:
        with open(referencia_csv) as f:
            for fila in csv.DictReader(f):
                referencia[fila["kpi"]] = float(fila["valor_notebook"])
    except FileNotFoundError:
        return spark.createDataFrame(
            [("referencia_csv_no_encontrado", 0.0, 0.0, 0.0, False)],
            ["kpi", "valor_notebook", "valor_pipeline", "diferencia", "ok"],
        )

    rows = []
    for kpi, val_pipe in pipeline_vals.items():
        val_ref = referencia.get(kpi, None)
        if val_ref is None:
            continue
        diff = abs(val_pipe - val_ref)
        rel  = diff / val_ref if val_ref != 0 else diff
        rows.append((kpi, val_ref, val_pipe, diff, rel < 0.01))

    return spark.createDataFrame(rows, ["kpi", "valor_notebook", "valor_pipeline", "diferencia", "ok"])


def main():
    spark = get_spark("olist-pruebas")
    spark.sparkContext.setLogLevel("ERROR")

    REF = "mediciones/etapa2/eda/referencia_kpis.csv"
    fallos = 0

    checks = [
        ("unicidad silver.pedidos",    unicidad_claves(spark, "lake.silver.pedidos", ["order_id"])),
        ("unicidad silver.envios",     unicidad_claves(spark, "lake.silver.envios", ["order_id", "seller_id"])),
        ("unicidad silver.geo_cep",    unicidad_claves(spark, "lake.silver.geo_cep", ["zip"])),
        ("unicidad gold.pedidos",      unicidad_claves(spark, "lake.gold.pedidos", ["order_id"])),
        ("unicidad gold.vendedores",   unicidad_claves(spark, "lake.gold.vendedores", ["seller_id"])),
        ("orden_fechas",               orden_fechas(spark)),
        ("identidad_descomposicion",   identidad_descomposicion(spark)),
        ("coherencia_eda",             coherencia_eda(spark, REF)),
    ]

    for nombre, resultado in checks:
        n = resultado.count()
        estado = "OK" if n == 0 else f"FALLO ({n} filas)"
        print(f"  {nombre:<35} {estado}")
        if n > 0:
            resultado.show(5, truncate=False)
            fallos += 1

    spark.stop()
    if fallos > 0:
        print(f"\n{fallos} prueba(s) fallaron.")
        sys.exit(1)
    print("\nTodas las pruebas pasaron.")


if __name__ == "__main__":
    main()
