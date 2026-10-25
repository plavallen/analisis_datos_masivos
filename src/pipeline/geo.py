from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from src.pipeline.config import R_TIERRA_KM, TRAMOS


def haversine_km(lat1, lng1, lat2, lng2):
    """Distancia en km entre dos pares de coordenadas (columnas Spark)."""
    dlat = F.radians(lat2 - lat1)
    dlng = F.radians(lng2 - lng1)
    a = (
        F.pow(F.sin(dlat / 2), 2)
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlng / 2), 2)
    )
    return F.lit(2 * R_TIERRA_KM) * F.asin(F.sqrt(a))


def calcular_geo_cep(geo_limpio: DataFrame) -> DataFrame:
    """
    Calcula un centroide (mediana lat/lng) por prefijo postal.
    geo_limpio debe tener columnas: zip, geolocation_lat, geolocation_lng.
    """
    return (
        geo_limpio
        .groupBy("zip")
        .agg(
            F.count("*").alias("n_puntos"),
            F.percentile_approx("geolocation_lat", 0.5).alias("lat_med"),
            F.percentile_approx("geolocation_lng", 0.5).alias("lng_med"),
        )
        .withColumn(
            "geo_confianza",
            F.when(F.col("n_puntos") >= 3, "ok").otherwise("baja"),
        )
    )


def calcular_ciudades_geo(geo_cep: DataFrame, customers: DataFrame, sellers: DataFrame) -> DataFrame:
    """
    Calcula el centro por ciudad_uf como la mediana de los centroides de sus prefijos.
    Requiere que customers y sellers ya tengan city_canon y ciudad_uf.
    """
    ciudades_zip = (
        customers.select(
            F.col("customer_zip_code_prefix").alias("zip"),
            F.col("ciudad_uf"),
        ).union(
            sellers.select(
                F.col("seller_zip_code_prefix").alias("zip"),
                F.col("ciudad_uf"),
            )
        ).distinct()
    )

    return (
        ciudades_zip
        .join(geo_cep.select("zip", "lat_med", "lng_med"), "zip", "inner")
        .groupBy("ciudad_uf")
        .agg(
            F.count("*").alias("n_prefijos"),
            F.percentile_approx("lat_med", 0.5).alias("lat"),
            F.percentile_approx("lng_med", 0.5).alias("lng"),
        )
    )


def agregar_distancia(df: DataFrame, geo_cep: DataFrame) -> DataFrame:
    """
    Agrega distancia_km y distancia_calidad a un DataFrame que tenga
    orig_zip y dest_zip.
    """
    geo_orig = F.broadcast(
        geo_cep.select(
            F.col("zip").alias("orig_zip"),
            F.col("lat_med").alias("orig_lat"),
            F.col("lng_med").alias("orig_lng"),
            F.col("geo_confianza").alias("conf_orig"),
        )
    )
    geo_dest = F.broadcast(
        geo_cep.select(
            F.col("zip").alias("dest_zip"),
            F.col("lat_med").alias("dest_lat"),
            F.col("lng_med").alias("dest_lng"),
            F.col("geo_confianza").alias("conf_dest"),
        )
    )

    return (
        df.join(geo_orig, "orig_zip", "left")
        .join(geo_dest, "dest_zip", "left")
        .withColumn(
            "distancia_km",
            F.when(
                F.col("orig_lat").isNotNull() & F.col("dest_lat").isNotNull(),
                haversine_km(
                    F.col("orig_lat"), F.col("orig_lng"),
                    F.col("dest_lat"), F.col("dest_lng"),
                ),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "distancia_calidad",
            F.when(F.col("distancia_km").isNull(), "sin_geo")
            .when(
                (F.col("conf_orig") == "baja") | (F.col("conf_dest") == "baja"),
                "baja",
            )
            .otherwise("ok"),
        )
        .drop("orig_lat", "orig_lng", "dest_lat", "dest_lng", "conf_orig", "conf_dest")
    )


def agregar_tramo_distancia(df: DataFrame, col: str = "distancia_km") -> DataFrame:
    """Agrega columna tramo_distancia según los cortes definidos en config."""
    expr = None
    for limite, nombre in TRAMOS:
        if expr is None:
            if limite is not None:
                expr = F.when(F.col(col) < limite, nombre)
            else:
                expr = F.when(F.lit(True), nombre)
        else:
            if limite is not None:
                expr = expr.when(F.col(col) < limite, nombre)
            else:
                expr = expr.otherwise(nombre)
    return df.withColumn("tramo_distancia", expr)
