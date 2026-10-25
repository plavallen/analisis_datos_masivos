from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def _horas(fin, inicio):
    return (F.unix_timestamp(fin) - F.unix_timestamp(inicio)) / 3600.0


def _dias(fin, inicio):
    return (F.unix_timestamp(fin) - F.unix_timestamp(inicio)) / 86400.0


def agregar_tiempos(df: DataFrame) -> DataFrame:
    """
    Agrega columnas de tiempo del ciclo de vida del pedido.
    Requiere los timestamps del esquema orders.
    """
    return (
        df
        .withColumn("t_aprobacion",
            _horas("order_approved_at", "order_purchase_timestamp"))
        .withColumn("t_despacho",
            _dias("order_delivered_carrier_date", "order_approved_at"))
        .withColumn("t_transito",
            _dias("order_delivered_customer_date", "order_delivered_carrier_date"))
        .withColumn("t_total",
            _dias("order_delivered_customer_date", "order_purchase_timestamp"))
        .withColumn("dias_atraso",
            F.datediff(
                F.to_date("order_delivered_customer_date"),
                F.to_date("order_estimated_delivery_date"),
            ))
        .withColumn("tarde",
            F.when(F.col("dias_atraso").isNull(), F.lit(None).cast("boolean"))
             .otherwise(F.col("dias_atraso") > 0))
        .withColumn("holgura_promesa",
            F.datediff(
                F.to_date("order_estimated_delivery_date"),
                F.to_date("order_delivered_customer_date"),
            ))
        .withColumn("dias_prometidos",
            F.datediff(
                F.to_date("order_estimated_delivery_date"),
                F.to_date("order_purchase_timestamp"),
            ))
        .withColumn("mes_compra",
            F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .withColumn("fecha_compra",
            F.to_date("order_purchase_timestamp"))
    )


def agregar_excesos(df: DataFrame, items_df: DataFrame) -> DataFrame:
    """
    Agrega exceso_vendedor, exceso_transporte y despacho_fuera_plazo.
    items_df debe tener order_id y shipping_limit_date.
    exceso_vendedor + exceso_transporte = dias_atraso para tardíos.
    """
    limite = (
        items_df
        .groupBy("order_id")
        .agg(F.max("shipping_limit_date").alias("shipping_limit_date"))
    )

    return (
        df.join(limite, "order_id", "left")
        .withColumn("dias_despacho_vs_limite",
            _dias("order_delivered_carrier_date", "shipping_limit_date"))
        .withColumn("despacho_fuera_plazo",
            F.when(
                F.col("order_delivered_carrier_date").isNotNull()
                & F.col("shipping_limit_date").isNotNull(),
                F.col("dias_despacho_vs_limite") > 0,
            ).otherwise(F.lit(None).cast("boolean")))
        .withColumn("exceso_vendedor",
            F.when(F.col("tarde") == True,
                F.greatest(F.lit(0.0), F.col("dias_despacho_vs_limite")))
             .otherwise(F.lit(0.0)))
        .withColumn("exceso_transporte",
            F.when(F.col("tarde") == True,
                F.col("dias_atraso").cast("double") - F.col("exceso_vendedor"))
             .otherwise(F.lit(0.0)))
        .drop("shipping_limit_date", "dias_despacho_vs_limite")
    )


def agregar_flags_temporales(df: DataFrame) -> DataFrame:
    """Agrega flags booleanos para inconsistencias en fechas del ciclo."""
    return (
        df
        .withColumn("flag_aprobacion_antes_compra",
            F.col("t_aprobacion") < 0)
        .withColumn("flag_despacho_antes_aprobacion",
            F.col("t_despacho") < 0)
        .withColumn("flag_entrega_antes_despacho",
            F.col("t_transito") < 0)
        .withColumn("flag_entrega_antes_compra",
            F.col("t_total") < 0)
        .withColumn("flag_promesa_antes_compra",
            F.col("dias_prometidos") < 0)
    )
