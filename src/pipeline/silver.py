"""Carga y transforma bronze en tablas Iceberg en lake.silver."""
from pyspark.sql import Window
import pyspark.sql.functions as F

from src.pipeline.config import (
    get_spark, BRONZE_BASE, REGIONES, PERIODO_INICIO, PERIODO_FIN,
)
from src.pipeline.limpieza import (
    normalizar_zip, canonicalizar_ciudad, agregar_ciudad_uf,
    limpiar_geolocation, a_cuarentena,
)
from src.pipeline.geo import (
    calcular_geo_cep, calcular_ciudades_geo, agregar_distancia, agregar_tramo_distancia,
)
from src.pipeline.entregas import agregar_tiempos, agregar_excesos, agregar_flags_temporales
from src.pipeline.resenas import una_resena_por_pedido, agregar_flags_resena


def _write(df, table):
    (df.writeTo(table)
       .tableProperty("write.format.default", "parquet")
       .createOrReplace())
    print(f"  {table}: {df.count():,} filas")


def main():
    spark = get_spark("olist-silver")
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")

    print("Leyendo bronze...")
    orders          = spark.read.parquet(f"{BRONZE_BASE}/orders")
    order_items     = spark.read.parquet(f"{BRONZE_BASE}/order_items")
    order_payments  = spark.read.parquet(f"{BRONZE_BASE}/order_payments")
    order_reviews   = spark.read.parquet(f"{BRONZE_BASE}/order_reviews")
    customers       = spark.read.parquet(f"{BRONZE_BASE}/customers")
    sellers         = spark.read.parquet(f"{BRONZE_BASE}/sellers")
    products        = spark.read.parquet(f"{BRONZE_BASE}/products")
    cat_translation = spark.read.parquet(f"{BRONZE_BASE}/category_translation")
    geolocation     = spark.read.parquet(f"{BRONZE_BASE}/geolocation")

    mapa_region = spark.createDataFrame(REGIONES, ["uf", "region"])

    print("\n[geo_cep]")
    geo_cep = calcular_geo_cep(limpiar_geolocation(geolocation)).cache()
    _write(geo_cep, "lake.silver.geo_cep")

    print("\n[customers]")
    customers_s = (
        normalizar_zip(customers, "customer_zip_code_prefix")
        .transform(lambda df: canonicalizar_ciudad(df, "customer_city", "customer_state"))
        .transform(lambda df: agregar_ciudad_uf(df, "city_canon", "customer_state"))
        .join(
            mapa_region.withColumnRenamed("uf", "customer_state")
                       .withColumnRenamed("region", "customer_region"),
            "customer_state", "left",
        )
        .withColumn("customer_region", F.coalesce("customer_region", F.lit("Otra")))
    )
    _write(customers_s, "lake.silver.customers")

    print("\n[sellers]")
    sellers_s = (
        normalizar_zip(sellers, "seller_zip_code_prefix")
        .transform(lambda df: canonicalizar_ciudad(df, "seller_city", "seller_state"))
        .transform(lambda df: agregar_ciudad_uf(df, "city_canon", "seller_state"))
        .join(
            mapa_region.withColumnRenamed("uf", "seller_state")
                       .withColumnRenamed("region", "seller_region"),
            "seller_state", "left",
        )
        .withColumn("seller_region", F.coalesce("seller_region", F.lit("Otra")))
    )
    _write(sellers_s, "lake.silver.sellers")

    print("\n[ciudades_geo]")
    _write(calcular_ciudades_geo(geo_cep, customers_s, sellers_s), "lake.silver.ciudades_geo")

    print("\n[products]")
    products_s = (
        products
        .withColumnRenamed("product_name_lenght",        "product_name_length")
        .withColumnRenamed("product_description_lenght", "product_description_length")
        .join(cat_translation, "product_category_name", "left")
        .withColumn("product_category_en",
            F.coalesce("product_category_name_english", F.lit("sin_categoria")))
        .withColumn("volumen_cm3",
            F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"))
        .withColumn("flag_peso_invalido",
            F.col("product_weight_g").isNull() | (F.col("product_weight_g") == 0))
        .withColumn("flag_dim_invalida",
            F.col("volumen_cm3").isNull() | (F.col("volumen_cm3") <= 0))
    )
    _write(products_s, "lake.silver.products")

    print("\n[order_reviews]")
    reviews_s = (
        una_resena_por_pedido(order_reviews)
        .transform(lambda df: agregar_flags_resena(df, orders))
    )
    _write(reviews_s, "lake.silver.order_reviews")

    print("\n[order_payments]")
    payments_s = (
        order_payments
        .withColumn("flag_installments_cero", F.col("payment_installments") == 0)
        .withColumn("flag_tipo_indefinido",   F.col("payment_type") == "not_defined")
    )
    _write(payments_s, "lake.silver.order_payments")

    print("\n[order_items]")
    q1_p, q3_p = order_items.approxQuantile("price",         [0.25, 0.75], 0.01)
    q1_f, q3_f = order_items.approxQuantile("freight_value", [0.25, 0.75], 0.01)
    items_s = (
        order_items
        .withColumn("flag_precio_extremo", F.col("price")         > q3_p + 3.0 * (q3_p - q1_p))
        .withColumn("flag_flete_extremo",  F.col("freight_value") > q3_f + 3.0 * (q3_f - q1_f))
        .withColumn("flag_precio_cero",    F.col("price") <= 0)
    )
    _write(items_s, "lake.silver.order_items")

    print("\n[orders]")
    orders_s = (
        agregar_tiempos(orders)
        .withColumn("en_periodo_util",
            (F.col("mes_compra") >= PERIODO_INICIO) & (F.col("mes_compra") <= PERIODO_FIN))
        .transform(agregar_flags_temporales)
        .transform(lambda df: agregar_excesos(df, order_items))
    )
    _write(orders_s, "lake.silver.orders")

    print("\n[cuarentena]")
    cuarentena_rows = (
        orders_s
        .filter(
            F.col("flag_despacho_antes_aprobacion")
            | F.col("flag_entrega_antes_despacho")
            | F.col("flag_entrega_antes_compra")
        )
        .transform(lambda df: a_cuarentena(df, "orders", "ORD_FECHA_ORDEN", "fechas del ciclo fuera de orden"))
    )
    _write(cuarentena_rows, "lake.silver.cuarentena")

    w_pago = Window.partitionBy("order_id").orderBy(F.desc("payment_value"), F.col("payment_sequential"))
    pago_principal = (
        order_payments
        .withColumn("_rn", F.row_number().over(w_pago))
        .filter(F.col("_rn") == 1)
        .select("order_id", "payment_type", "payment_value")
    )
    pago_total_df = (
        order_payments
        .groupBy("order_id")
        .agg(F.sum("payment_value").alias("pago_total"))
    )
    items_agg = (
        items_s
        .join(
            products_s.select("product_id", "product_category_en", "product_weight_g", "volumen_cm3", "flag_peso_invalido"),
            "product_id", "left",
        )
        .groupBy("order_id")
        .agg(
            F.count("*").alias("n_items"),
            F.countDistinct("seller_id").alias("n_vendedores"),
            F.sum("price").alias("precio_total"),
            F.sum("freight_value").alias("flete_total"),
            F.avg("product_weight_g").alias("peso_medio_g"),
            F.avg("volumen_cm3").alias("volumen_medio_cm3"),
            F.max(F.struct(F.col("price"), F.col("product_category_en"))).alias("_top_item"),
            F.sum(F.col("flag_peso_invalido").cast("int")).alias("_n_peso_inv"),
        )
        .withColumn("categoria_principal", F.col("_top_item.product_category_en"))
        .withColumn("flag_peso_invalido", F.col("_n_peso_inv") > 0)
        .drop("_top_item", "_n_peso_inv")
    )
    seller_principal = (
        items_s
        .groupBy("order_id")
        .agg(F.max(F.struct(F.col("price"), F.col("seller_id"))).alias("_top"))
        .withColumn("seller_id", F.col("_top.seller_id"))
        .drop("_top")
    )

    print("\n[silver.pedidos]")
    pedidos = (
        orders_s
        .join(
            customers_s.select(
                "customer_id", "customer_unique_id",
                "customer_state", "customer_region",
                "customer_zip_code_prefix",
                F.col("ciudad_uf").alias("ciudad_uf_cliente"),
            ),
            "customer_id", "left",
        )
        .join(
            geo_cep.select(
                F.col("zip").alias("customer_zip_code_prefix"),
                F.col("lat_med").alias("customer_lat"),
                F.col("lng_med").alias("customer_lng"),
                F.col("geo_confianza").alias("geo_confianza_cliente"),
            ),
            "customer_zip_code_prefix", "left",
        )
        .join(seller_principal, "order_id", "left")
        .join(items_agg, "order_id", "left")
        .join(pago_principal.select("order_id", "payment_type"), "order_id", "left")
        .join(pago_total_df, "order_id", "left")
        .join(
            reviews_s.select("order_id", "review_score", "review_negativa", "resena_antes_entrega"),
            "order_id", "left",
        )
        .withColumn("gmv", F.col("precio_total") + F.col("flete_total"))
        .withColumn("diff_pago", F.col("pago_total") - F.col("gmv"))
        .withColumn("multi_vendedor", F.col("n_vendedores") > 1)
        .withColumn("transito_asignado",
            F.col("dias_prometidos").cast("double")
            - F.col("t_aprobacion") / 24.0
            - F.col("exceso_vendedor"))
    )
    _write(pedidos, "lake.silver.pedidos")

    print("\n[silver.envios]")
    pedido_base = pedidos.select(
        "order_id", "order_status", "tarde", "dias_atraso", "review_score", "review_negativa",
    )
    envios_agg = (
        order_items
        .join(products_s.select("product_id", "product_category_en"), "product_id", "left")
        .groupBy("order_id", "seller_id")
        .agg(
            F.count("*").alias("n_items"),
            F.sum("price").alias("precio"),
            F.sum("freight_value").alias("flete"),
            F.max(F.struct(F.col("price"), F.col("product_category_en"))).alias("_top"),
        )
        .withColumn("categoria_principal", F.col("_top.product_category_en"))
        .drop("_top")
    )
    envios_full = (
        envios_agg
        .join(pedido_base, "order_id", "inner")
        .join(orders.select("order_id", "customer_id"), "order_id", "inner")
        .join(
            customers_s.select(
                "customer_id", "customer_state", "customer_region",
                "customer_zip_code_prefix",
                F.col("ciudad_uf").alias("ciudad_uf_cliente"),
            ),
            "customer_id", "left",
        )
        .join(
            sellers_s.select(
                "seller_id", "seller_state", "seller_region",
                "seller_zip_code_prefix",
                F.col("ciudad_uf").alias("ciudad_uf_vendedor"),
            ),
            "seller_id", "left",
        )
        .join(
            geo_cep.select(
                F.col("zip").alias("customer_zip_code_prefix"),
                F.col("lat_med").alias("customer_lat"),
                F.col("lng_med").alias("customer_lng"),
            ),
            "customer_zip_code_prefix", "left",
        )
        .join(
            geo_cep.select(
                F.col("zip").alias("seller_zip_code_prefix"),
                F.col("lat_med").alias("seller_lat"),
                F.col("lng_med").alias("seller_lng"),
            ),
            "seller_zip_code_prefix", "left",
        )
        .withColumn("orig_zip", F.col("seller_zip_code_prefix"))
        .withColumn("dest_zip", F.col("customer_zip_code_prefix"))
        .transform(lambda df: agregar_distancia(df, geo_cep))
        .transform(agregar_tramo_distancia)
        .withColumn("ruta_estado",
            F.concat_ws(" -> ", F.col("seller_state"), F.col("customer_state")))
        .withColumn("ruta_ciudad",
            F.concat_ws(" -> ", F.col("ciudad_uf_vendedor"), F.col("ciudad_uf_cliente")))
        .withColumn("intraurbana", F.col("ciudad_uf_vendedor") == F.col("ciudad_uf_cliente"))
        .withColumn("mismo_estado", F.col("seller_state") == F.col("customer_state"))
        .drop("orig_zip", "dest_zip", "customer_id",
              "customer_zip_code_prefix", "seller_zip_code_prefix")
    )
    _write(envios_full, "lake.silver.envios")

    max_diff = (
        pedidos
        .filter(F.col("tarde") == True)
        .select(F.abs(
            F.col("dias_atraso").cast("double")
            - F.col("exceso_vendedor")
            - F.col("exceso_transporte")
        ).alias("diff"))
        .agg(F.max("diff").alias("max_diff"))
        .collect()[0]["max_diff"]
    )
    print(f"\nVerificación identidad exceso_vendedor + exceso_transporte = dias_atraso")
    print(f"  Diferencia máxima: {max_diff:.6f} días (debe ser ~0)")

    spark.stop()
    print("\nSilver completado.")


if __name__ == "__main__":
    main()
