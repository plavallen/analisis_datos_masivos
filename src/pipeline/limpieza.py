from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window


def normalizar_zip(df: DataFrame, col: str) -> DataFrame:
    """Normaliza prefijo postal a 5 dígitos con ceros a la izquierda."""
    return df.withColumn(col, F.lpad(F.col(col).cast("string"), 5, "0"))


def canon_col(col_name: str):
    """Expresión de canonicalización de ciudad para una columna."""
    return F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.translate(
                    F.lower(F.trim(F.col(col_name))),
                    "áàäâãéèëêíìïîóòöôõúùüûçñãõ",
                    "aaaaaeeeeiiiiooooouuuucnaao",
                ),
                r"[''\-]", " ",
            ),
            r"\s+", " ",
        )
    )


def canonicalizar_ciudad(df: DataFrame, col_ciudad: str, col_uf: str) -> DataFrame:
    """Agrega columna city_canon con nombre de ciudad normalizado."""
    return df.withColumn("city_canon", canon_col(col_ciudad))


def agregar_ciudad_uf(df: DataFrame, col_ciudad_canon: str = "city_canon", col_uf: str = None) -> DataFrame:
    """Agrega columna ciudad_uf como 'ciudad_canon|UF'."""
    return df.withColumn(
        "ciudad_uf",
        F.concat_ws("|", F.col(col_ciudad_canon), F.col(col_uf)),
    )


def limpiar_geolocation(df: DataFrame) -> DataFrame:
    """
    Filtra puntos fuera del bounding box de Brasil y filas cuyo estado
    difiere del estado mayoritario de su prefijo.
    """
    from src.pipeline.config import LAT_MIN, LAT_MAX, LNG_MIN, LNG_MAX

    geo = df.withColumn(
        "zip", F.lpad(F.col("geolocation_zip_code_prefix").cast("string"), 5, "0")
    )

    en_brasil = (
        F.col("geolocation_lat").between(LAT_MIN, LAT_MAX)
        & F.col("geolocation_lng").between(LNG_MIN, LNG_MAX)
    )

    por_zip_uf = geo.groupBy("zip", "geolocation_state").count()
    moda = (
        por_zip_uf
        .withColumn(
            "r",
            F.row_number().over(
                Window.partitionBy("zip").orderBy(F.desc("count"))
            ),
        )
        .filter("r = 1")
        .select("zip", F.col("geolocation_state").alias("uf_moda"))
    )

    disid = (
        geo.join(moda, "zip")
        .filter(F.col("geolocation_state") != F.col("uf_moda"))
        .select("zip", "geolocation_lat", "geolocation_lng")
    )

    return (
        geo.filter(en_brasil)
        .join(disid, ["zip", "geolocation_lat", "geolocation_lng"], "left_anti")
        .select("zip", "geolocation_lat", "geolocation_lng", "geolocation_state")
        .distinct()
    )


def a_cuarentena(df: DataFrame, tabla_origen: str, regla_id: str, motivo: str) -> DataFrame:
    """Agrega columnas de trazabilidad de cuarentena."""
    return (
        df.withColumn("tabla_origen", F.lit(tabla_origen))
        .withColumn("regla_id",      F.lit(regla_id))
        .withColumn("motivo",        F.lit(motivo))
    )
