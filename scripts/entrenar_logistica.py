"""
Script de entrenamiento reproducible para el modelo de regresión logística.
Se ejecuta con spark-submit y reproduce las métricas de prueba de la notebook.

Uso:
    spark-submit \\
        --master spark://spark-master:7077 \\
        --jars /opt/spark/extra-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar,... \\
        /repo/etapa2/scripts/entrenar_logistica.py
"""

import sys
import os

sys.path.insert(0, '/repo/etapa2/src')
sys.path.insert(0, '/repo/etapa2/lib/sparkstats/src')

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import mlflow

from modelo.config import (
    FECHA_FIN_TRAIN, FECHA_FIN_VAL, FECHA_FIN_TEST,
    EXPERIMENTO_MLFLOW, DIR_GRAFICOS,
)
from modelo.datos import leer_silver, filtrar_poblacion
from modelo.features import envio_critico, variables_pedido, historial_vendedor, margen_promesa
from modelo.entrenar import busqueda_hiperparametros, ablacion, referencias, entrenar_final
from modelo.evaluacion import evaluacion_completa


JARS = ','.join([
    '/opt/spark/extra-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar',
    '/opt/spark/extra-jars/hadoop-aws-3.4.2.jar',
    '/opt/spark/extra-jars/aws-sdk-bundle-v2-2.29.52.jar',
])

VARIABLES_FINALES = [
    'dias_prometidos', 'margen_promesa', 'distancia_critica_km',
    'precio_total', 'flete_total', 'peso_medio_g', 'volumen_medio_cm3',
    'diff_pago', 'vol_seller_id_critico_90d', 'tasa_seller_id_critico_90d',
    'n_items', 'mes_del_anio', 'dia_semana',
    'customer_state', 'seller_state_critico', 'categoria_principal',
    'payment_type', 'mismo_estado', 'intraurbana', 'multi_vendedor',
    'flag_peso_invalido', 'distancia_incompleta',
]


def main():
    spark = (
        SparkSession.builder
        .appName('entrenar_logistica')
        .config('spark.jars', JARS)
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.sql.catalog.lake', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.lake.type', 'hadoop')
        .config('spark.sql.catalog.lake.warehouse', 's3a://olist/warehouse')
        .config('spark.hadoop.fs.s3a.endpoint', os.environ.get('RUSTFS_ENDPOINT', 'http://rustfs:9000'))
        .config('spark.hadoop.fs.s3a.access.key', os.environ['AWS_ACCESS_KEY_ID'])
        .config('spark.hadoop.fs.s3a.secret.key', os.environ['AWS_SECRET_ACCESS_KEY'])
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.sql.shuffle.partitions', '8')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')

    os.makedirs(DIR_GRAFICOS, exist_ok=True)

    # 1. Lectura de silver
    pedidos, envios, snapshots = leer_silver(spark)

    # 2. Población
    df_poblacion = filtrar_poblacion(pedidos)

    # 3 y 4. Features
    envios_crit = envio_critico(envios)
    df_vars = variables_pedido(df_poblacion, envios_crit)
    df_hist = historial_vendedor(df_vars)
    df_features = margen_promesa(df_hist)

    # 5. Partición temporal
    train = df_features.filter(F.col('fecha_compra') <= FECHA_FIN_TRAIN)
    val   = df_features.filter(
        (F.col('fecha_compra') > FECHA_FIN_TRAIN) & (F.col('fecha_compra') <= FECHA_FIN_VAL)
    )
    test  = df_features.filter(
        (F.col('fecha_compra') > FECHA_FIN_VAL) & (F.col('fecha_compra') <= FECHA_FIN_TEST)
    )

    print(f'Train: {train.count():,}, Val: {val.count():,}, Test: {test.count():,}')

    mlflow.set_experiment(EXPERIMENTO_MLFLOW)

    # 9. Búsqueda de hiperparámetros
    mejor_comb, historial = busqueda_hiperparametros(train, val, snapshots, VARIABLES_FINALES)
    print('Mejor combinación:', mejor_comb)

    # 10. Ablación
    ablacion_df = ablacion(mejor_comb, train, val)

    # 11. Referencias
    refs = referencias(train, val)
    print('Referencias:', refs)

    # 12. Modelo final
    modelo_final, run_id = entrenar_final(
        mejor_comb, train, val, snapshots, VARIABLES_FINALES, ablacion_df
    )

    # Evaluación en prueba
    metricas_prueba = evaluacion_completa(modelo_final, test, DIR_GRAFICOS)
    print('\nMétricas de prueba:')
    for k, v in metricas_prueba.items():
        if not isinstance(v, (list, dict)):
            print(f'  {k}: {v}')

    spark.stop()


if __name__ == '__main__':
    main()
