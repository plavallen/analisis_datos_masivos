"""
Test obligatorio del paso 4: un pedido entregado el mismo día de la compra,
o después, no debe entrar en el historial de esa compra.
"""

import pytest
from datetime import date, timedelta
from sparkstats.temporal import tasa_historica_suavizada


def test_no_filtrar_futuro(spark):
    """
    Pedidos entregados en la misma fecha de compra o después no deben
    aparecer en el historial usado para calcular la tasa de ese día.

    Escenario:
    - Vendedor V1 tiene 3 pedidos históricos: 2 tardíos, entregados el día -10.
    - El pedido a predecir se compra el día 0.
    - Un cuarto pedido de V1 se entrega el día 0 (misma fecha) y es tardío.
    - El historial de V1 para el día 0 debe incluir solo los 3 anteriores.
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

    dia_base = date(2018, 1, 15)
    datos = [
        # (seller_id, order_id, fecha_entrega, fecha_compra, tarde)
        # históricos de V1
        ("V1", "o1", dia_base - timedelta(days=10), dia_base - timedelta(days=20), 1),
        ("V1", "o2", dia_base - timedelta(days=10), dia_base - timedelta(days=20), 1),
        ("V1", "o3", dia_base - timedelta(days=10), dia_base - timedelta(days=20), 0),
        # pedido entregado el mismo día de la compra que se predice -> NO debe entrar
        ("V1", "o4", dia_base, dia_base, 1),
        # pedido a predecir
        ("V1", "o5", None, dia_base, None),
    ]

    schema = StructType([
        StructField("seller_id", StringType()),
        StructField("order_id", StringType()),
        StructField("fecha_entrega", DateType()),
        StructField("fecha_compra", DateType()),
        StructField("tarde", IntegerType()),
    ])
    df = spark.createDataFrame(datos, schema=schema)

    resultado = tasa_historica_suavizada(
        df=df,
        col_clave="seller_id",
        col_objetivo="tarde",
        col_fecha_entrega="fecha_entrega",
        col_fecha_referencia="fecha_compra",
        ventana_dias=90,
        m_suavizado=30.0,
    )

    fila_pred = resultado.filter(
        (resultado["order_id"] == "o5")
    ).collect()

    assert len(fila_pred) == 1, "El pedido a predecir debe aparecer exactamente una vez"

    vol_col = "vol_seller_id_90d"
    vol = fila_pred[0][vol_col]

    # Solo deben contar o1, o2, o3 (entregados en dia_base - 10).
    # o4 se entrega el mismo día que la compra de o5, por lo tanto NO debe entrar.
    assert vol == 3, (
        f"Se esperaba volumen histórico = 3 (sin o4), se obtuvo {vol}"
    )
