"""
Construcción del Pipeline de PySpark ML con todos los transformadores.
"""

from __future__ import annotations

import yaml
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    QuantileDiscretizer,
    SQLTransformer,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.classification import LogisticRegression

from sparkstats.transformadores import AgruparRaras, RecortarPercentiles
from modelo.config import DIR_AGRUPAMIENTOS, MIN_EVENTOS_DEFAULT


# Variables que se aplican log1p (distribuciones con cola derecha larga)
COLS_LOG1P = [
    "precio_total", "flete_total", "peso_medio_g", "volumen_medio_cm3",
    "distancia_critica_km", "vol_seller_id_critico_90d",
]

# Numéricas que van al VectorAssembler (después de todas las transformaciones)
NUMERICAS_FINALES = [
    "dias_prometidos", "margen_promesa",
    "log1p_distancia_critica_km",
    "log1p_precio_total", "log1p_flete_total",
    "log1p_peso_medio_g", "log1p_volumen_medio_cm3",
    "diff_pago_recortado",
    "log1p_vol_seller_id_critico_90d",
    "tasa_seller_id_critico_90d",
]
DISCRETAS_FINALES = ["n_items", "mes_del_anio", "dia_semana"]
BINARIAS_FINALES  = [
    "mismo_estado", "multi_vendedor",
    "flag_peso_invalido", "distancia_incompleta",
]
CATEGORICAS_FINALES = [
    "customer_state", "seller_state_critico", "categoria_principal",
    "payment_type",
]


def cargar_agrupamientos() -> dict:
    """Carga el YAML de agrupamientos fijos desde DIR_AGRUPAMIENTOS."""
    ruta = Path(DIR_AGRUPAMIENTOS)
    if ruta.exists():
        with open(ruta) as f:
            return yaml.safe_load(f)
    return {}


def sql_agrupamientos_fijos(agrupamientos: dict) -> list[SQLTransformer]:
    """
    Construye SQLTransformers para aplicar los agrupamientos del YAML.
    Cada transformador reemplaza categorías según el mapa del YAML.
    """
    etapas = []
    for col, mapa in agrupamientos.get("columnas", {}).items():
        cases = "\n".join(
            f"  WHEN `{col}` = '{v}' THEN '{nuevo}'"
            for v, nuevo in mapa.items()
        )
        sql = f"SELECT *, CASE {cases} ELSE `{col}` END AS `{col}_ag` FROM __THIS__"
        etapas.append(SQLTransformer(statement=sql))
    return etapas


def sql_log1p() -> SQLTransformer:
    """SQLTransformer que aplica log1p a las columnas definidas en COLS_LOG1P."""
    transformaciones = ", ".join(
        f"log(1 + COALESCE(`{c}`, 0)) AS `log1p_{c}`" for c in COLS_LOG1P
    )
    return SQLTransformer(
        statement=f"SELECT *, {transformaciones} FROM __THIS__"
    )


def construir_pipeline(
    min_eventos: int = MIN_EVENTOS_DEFAULT,
    reg_param: float = 0.01,
    elastic_net_param: float = 0.5,
) -> Pipeline:
    """
    Arma el Pipeline completo de entrenamiento.

    Orden de etapas:
    1. Agrupamientos fijos del YAML
    2. AgruparRaras por cada columna categórica
    3. StringIndexer con handleInvalid="keep"
    4. OneHotEncoder con dropLast=True (categoría de referencia)
    5. log1p de numéricas con cola larga (SQLTransformer)
    6. RecortarPercentiles para diff_pago
    7. Imputer con mediana + columnas indicadoras de faltante
    8. VectorAssembler
    9. LogisticRegression elastic net sin ponderar clases

    Parámetros
    ----------
    min_eventos : umbral de AgruparRaras
    reg_param : parámetro de regularización de LogisticRegression
    elastic_net_param : mezcla L1/L2 (0=L2, 1=L1)
    """
    agrupamientos = cargar_agrupamientos()
    etapas_fijas = sql_agrupamientos_fijos(agrupamientos)

    etapas_raras = [
        AgruparRaras(
            inputCol=c, outputCol=f"{c}_ag",
            colObjetivo="tarde", minEventos=min_eventos,
        )
        for c in CATEGORICAS_FINALES
    ]

    cols_ag = [f"{c}_ag" for c in CATEGORICAS_FINALES]
    cols_idx = [f"{c}_idx" for c in CATEGORICAS_FINALES]
    cols_ohe = [f"{c}_ohe" for c in CATEGORICAS_FINALES]

    indexers = [
        StringIndexer(inputCol=c_ag, outputCol=c_idx, handleInvalid="keep")
        for c_ag, c_idx in zip(cols_ag, cols_idx)
    ]
    encoder = OneHotEncoder(inputCols=cols_idx, outputCols=cols_ohe, dropLast=True)

    log1p = sql_log1p()
    recortar = RecortarPercentiles(inputCol="diff_pago", outputCol="diff_pago_recortado")

    # Columnas numéricas que pueden tener nulos (historial y margen de promesa)
    cols_a_imputar = [
        "margen_promesa", "tasa_seller_id_critico_90d",
        "vol_seller_id_critico_90d", "log1p_distancia_critica_km",
    ]
    cols_ind_nulos = [f"ind_nulo_{c}" for c in cols_a_imputar]

    # Columnas indicadoras de faltante (antes de Imputer)
    sql_indicadores = "SELECT *, " + ", ".join(
        f"CAST(`{c}` IS NULL AS INT) AS `ind_nulo_{c}`" for c in cols_a_imputar
    ) + " FROM __THIS__"
    indicadores = SQLTransformer(statement=sql_indicadores)

    imputer = Imputer(
        inputCols=cols_a_imputar,
        outputCols=[f"{c}_imp" for c in cols_a_imputar],
        strategy="median",
    )

    cols_numericas_ensamble = (
        [f"{c}_imp" if c in cols_a_imputar else c for c in NUMERICAS_FINALES]
        + DISCRETAS_FINALES
        + cols_ind_nulos
    )

    assembler = VectorAssembler(
        inputCols=cols_numericas_ensamble + BINARIAS_FINALES + cols_ohe,
        outputCol="features",
        handleInvalid="skip",
    )

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="tarde",
        family="binomial",
        standardization=True,
        regParam=reg_param,
        elasticNetParam=elastic_net_param,
    )

    return Pipeline(stages=(
        etapas_fijas
        + etapas_raras
        + indexers
        + [encoder, log1p, recortar, indicadores, imputer, assembler, lr]
    ))
