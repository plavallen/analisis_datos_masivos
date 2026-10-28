"""
Diagnósticos de variables antes del modelado.
Usa la librería sparkstats para todos los cálculos genéricos.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from sparkstats.descriptiva import (
    resumen_numericas,
    tabla_frecuencias,
    entropia_normalizada,
    cardinalidad,
    nulos_por_conjunto,
)
from sparkstats.asociacion import cramer_v, matriz_cramer, matriz_correlacion
from sparkstats.intervalos import tasa_por_grupo
from sparkstats.relacion_objetivo import logit_empirico
from sparkstats.graficos import heatmap_matriz, box_robusto, barras_ordenadas


NUMERICAS = [
    "dias_prometidos", "margen_promesa", "distancia_critica_km",
    "precio_total", "flete_total", "peso_medio_g", "volumen_medio_cm3",
    "diff_pago", "vol_seller_id_critico_90d", "tasa_seller_id_critico_90d",
]
DISCRETAS = ["n_items", "n_vendedores", "n_estados_vendedor"]
CATEGORICAS = [
    "customer_state", "customer_region", "seller_state_critico",
    "categoria_principal", "payment_type", "tramo_critico",
    "mismo_estado", "intraurbana", "multi_vendedor",
    "flag_peso_invalido", "distancia_incompleta",
    "mes_del_anio", "dia_semana",
]


def diagnostico_nulos(
    train: DataFrame,
    val: DataFrame,
    test: DataFrame,
    columnas: List[str],
) -> DataFrame:
    """
    Porcentaje de nulos por variable en los tres conjuntos, en una sola tabla.

    También devuelve las categorías presentes en val o test que no están en train
    como segundo DataFrame.
    """
    return nulos_por_conjunto(
        {"entrenamiento": train, "validacion": val, "prueba": test},
        columnas,
    )


def diagnostico_categorias_nuevas(
    train: DataFrame,
    val: DataFrame,
    test: DataFrame,
    columnas: List[str],
) -> DataFrame:
    """
    Categorías presentes en val o test que no aparecen en train.
    Devuelve un DataFrame con columnas [columna, conjunto, categoria].
    """
    spark = train.sparkSession
    filas = []
    for col in columnas:
        cats_train = set(
            r[col] for r in train.select(col).distinct().collect() if r[col] is not None
        )
        for nombre, df in [("validacion", val), ("prueba", test)]:
            cats_df = set(
                r[col] for r in df.select(col).distinct().collect() if r[col] is not None
            )
            nuevas = cats_df - cats_train
            for c in nuevas:
                filas.append({"columna": col, "conjunto": nombre, "categoria": str(c)})
    return spark.createDataFrame(filas or [{"columna": "", "conjunto": "", "categoria": ""}],
                                  schema=["columna", "conjunto", "categoria"])


def diagnostico_numericas(df: DataFrame, carpeta: str) -> DataFrame:
    """Resumen estadístico y box plot robusto de columnas numéricas."""
    resumen = resumen_numericas(df, NUMERICAS, col_objetivo="tarde")
    box_robusto(df, NUMERICAS, titulo="Box plot robusto de numéricas", carpeta_salida=carpeta, nombre_archivo="box_numericas")
    return resumen


def diagnostico_discretas(df: DataFrame) -> dict:
    """Tablas de frecuencia con tasa de tardíos para variables discretas."""
    return {col: tabla_frecuencias(df, col, col_objetivo="tarde") for col in DISCRETAS}


def diagnostico_categoricas(df: DataFrame) -> dict:
    """
    Cardinalidad, entropía y tabla por categoría con tasa e intervalo de Wilson.
    """
    result = {}
    for col in CATEGORICAS:
        result[col] = {
            "entropia": entropia_normalizada(df, col),
            "tabla": tasa_por_grupo(df, col, "tarde"),
        }
    return result


def diagnostico_redundancia(df: DataFrame, carpeta: str) -> tuple[DataFrame, DataFrame, int]:
    """
    V de Cramér entre categóricas y Spearman entre numéricas.
    Devuelve (matriz_cramer_df, matriz_corr_df, n_filas_correlacion).
    """
    cat_cols = [c for c in CATEGORICAS if c in df.columns]
    mc = matriz_cramer(df, cat_cols)
    heatmap_matriz(mc, "col_a", "col_b", "cramer_v",
                   titulo="V de Cramér entre categóricas",
                   carpeta_salida=carpeta, nombre_archivo="heatmap_cramer")

    num_cols = [c for c in NUMERICAS if c in df.columns]
    mc2, n_filas = matriz_correlacion(df, num_cols, metodo="spearman")
    heatmap_matriz(mc2, "col_a", "col_b", "correlacion",
                   titulo=f"Correlación de Spearman entre numéricas (n={n_filas:,})",
                   carpeta_salida=carpeta, nombre_archivo="heatmap_spearman")

    return mc, mc2, n_filas


def diagnostico_relacion_objetivo(df: DataFrame, carpeta: str) -> None:
    """
    Logit empírico por deciles para numéricas y por valor para discretas.
    Tasa por categoría con Wilson para categóricas.
    """
    from sparkstats.graficos import curva, barras_ordenadas

    for col in NUMERICAS:
        if col not in df.columns:
            continue
        logit_df = logit_empirico(df, col, "tarde", n_cuantiles=10, por_valor=False)
        curva(logit_df, "grupo", "logit_empirico",
              titulo=f"Logit empírico: {col}",
              carpeta_salida=carpeta, nombre_archivo=f"logit_{col}")

    for col in DISCRETAS:
        if col not in df.columns:
            continue
        logit_df = logit_empirico(df, col, "tarde", por_valor=True)
        barras_ordenadas(logit_df, "grupo", "logit_empirico",
                         titulo=f"Logit empírico por valor: {col}",
                         carpeta_salida=carpeta, nombre_archivo=f"logit_val_{col}")
