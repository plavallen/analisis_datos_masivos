from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window


def una_resena_por_pedido(df: DataFrame) -> DataFrame:
    """Retiene la última reseña respondida por order_id."""
    w = Window.partitionBy("order_id").orderBy(F.desc("review_answer_timestamp"))
    return (
        df.withColumn("_rk", F.row_number().over(w))
        .filter(F.col("_rk") == 1)
        .drop("_rk")
    )


def agregar_flags_resena(resenas: DataFrame, orders: DataFrame) -> DataFrame:
    """
    Agrega review_negativa y resena_antes_entrega.
    resenas: resultado de una_resena_por_pedido.
    orders: debe tener order_id y order_delivered_customer_date.
    """
    fechas = orders.select("order_id", "order_delivered_customer_date")
    return (
        resenas
        .join(fechas, "order_id", "left")
        .withColumn("review_negativa",
            F.col("review_score") <= 2)
        .withColumn("resena_antes_entrega",
            F.col("review_answer_timestamp") < F.col("order_delivered_customer_date"))
        .drop("order_delivered_customer_date")
    )
