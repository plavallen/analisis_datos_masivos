"""
Métricas de evaluación para modelos de clasificación binaria sobre PySpark.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType


def log_loss(
    df: DataFrame,
    col_verdad: str,
    col_prob: str,
    epsilon: float = 1e-15,
) -> float:
    """
    Log-loss (entropía cruzada binaria).

    L = -mean(y * log(p) + (1-y) * log(1-p))

    Referencia: Murphy, "Probabilistic Machine Learning", 2022.

    Parámetros
    ----------
    df : DataFrame
    col_verdad : columna con la etiqueta real (0/1)
    col_prob : columna con la probabilidad predicha de la clase 1
    """
    eps = F.lit(epsilon)
    y = F.col(col_verdad).cast(DoubleType())
    p = F.greatest(eps, F.least(1.0 - eps, F.col(col_prob).cast(DoubleType())))
    resultado = df.agg(
        (-F.mean(y * F.log(p) + (1 - y) * F.log(1 - p))).alias("log_loss")
    ).collect()[0]["log_loss"]
    return float(resultado)


def brier(
    df: DataFrame,
    col_verdad: str,
    col_prob: str,
) -> float:
    """
    Puntaje de Brier: MSE entre probabilidad predicha y etiqueta real.

    B = mean((p - y)²)

    Referencia: Brier (1950), "Verification of Forecasts Expressed in Terms of Probability".
    """
    y = F.col(col_verdad).cast(DoubleType())
    p = F.col(col_prob).cast(DoubleType())
    resultado = df.agg(F.mean((p - y) ** 2).alias("brier")).collect()[0]["brier"]
    return float(resultado)


def tabla_calibracion(
    df: DataFrame,
    col_verdad: str,
    col_prob: str,
    n_deciles: int = 10,
) -> DataFrame:
    """
    Tabla de calibración por deciles del puntaje predicho.

    Devuelve un DataFrame con columnas:
        [decil, n, eventos, tasa_observada, prob_media, diferencia]

    Parámetros
    ----------
    df : DataFrame
    col_verdad : etiqueta real (0/1)
    col_prob : probabilidad predicha de la clase 1
    n_deciles : número de grupos
    """
    from pyspark.ml.feature import QuantileDiscretizer

    disc = QuantileDiscretizer(
        numBuckets=n_deciles,
        inputCol=col_prob,
        outputCol="__decil",
        relativeError=0.01,
        handleInvalid="skip",
    )
    df_dec = disc.fit(df).transform(df)

    return (
        df_dec.groupBy("__decil")
        .agg(
            F.count("*").alias("n"),
            F.sum(F.col(col_verdad).cast(DoubleType())).alias("eventos"),
            F.mean(F.col(col_verdad).cast(DoubleType())).alias("tasa_observada"),
            F.mean(F.col(col_prob).cast(DoubleType())).alias("prob_media"),
        )
        .withColumn("diferencia", F.col("prob_media") - F.col("tasa_observada"))
        .orderBy("__decil")
        .withColumnRenamed("__decil", "decil")
    )


def calibracion_global(
    df: DataFrame,
    col_verdad: str,
    col_prob: str,
) -> tuple[float, float]:
    """
    Calibración global: promedio predicho vs. tasa observada.

    Devuelve (prob_media, tasa_observada).
    """
    fila = df.agg(
        F.mean(F.col(col_prob).cast(DoubleType())).alias("prob_media"),
        F.mean(F.col(col_verdad).cast(DoubleType())).alias("tasa_observada"),
    ).collect()[0]
    return float(fila["prob_media"]), float(fila["tasa_observada"])


def pendiente_calibracion(
    df: DataFrame,
    col_verdad: str,
    col_prob: str,
    epsilon: float = 1e-15,
) -> float:
    """
    Pendiente de calibración: coeficiente de una regresión logística sin regularizar
    de col_verdad sobre el logit de col_prob.

    Pendiente = 1 indica calibración perfecta; < 1 indica sobreconfianza.

    Referencia: Van Calster et al. (2016), "Calibration in logistic regression".
    """
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import VectorAssembler

    eps = F.lit(epsilon)
    p = F.greatest(eps, F.least(1.0 - eps, F.col(col_prob).cast(DoubleType())))
    df_logit = df.withColumn("__logit", F.log(p / (1.0 - p)))

    assembler = VectorAssembler(inputCols=["__logit"], outputCol="__feat")
    df_vec = assembler.transform(df_logit)

    lr = LogisticRegression(
        featuresCol="__feat",
        labelCol=col_verdad,
        predictionCol="__pred_cal",
        rawPredictionCol="__rawpred_cal",
        probabilityCol="__prob_cal",
        regParam=0.0,
        elasticNetParam=0.0,
        maxIter=200,
    )
    modelo = lr.fit(df_vec)
    return float(modelo.coefficients[0])


def lift_por_decil(
    df: DataFrame,
    col_verdad: str,
    col_prob: str,
    n_deciles: int = 10,
) -> DataFrame:
    """
    Lift por decil del puntaje predicho (decil 1 = mayor probabilidad).

    Lift = tasa_observada_en_decil / tasa_global

    Devuelve un DataFrame con columnas [decil, n, eventos, tasa, lift].
    """
    from pyspark.sql.window import Window

    prevalencia = df.agg(F.mean(F.col(col_verdad).cast(DoubleType())).alias("prev")).collect()[0]["prev"]

    from pyspark.ml.feature import QuantileDiscretizer

    disc = QuantileDiscretizer(
        numBuckets=n_deciles,
        inputCol=col_prob,
        outputCol="__decil_raw",
        relativeError=0.01,
        handleInvalid="skip",
    )
    df_dec = disc.fit(df).transform(df)
    max_decil = df_dec.agg(F.max("__decil_raw")).collect()[0][0]

    return (
        df_dec
        .withColumn("decil", F.lit(max_decil) - F.col("__decil_raw") + 1)
        .groupBy("decil")
        .agg(
            F.count("*").alias("n"),
            F.sum(F.col(col_verdad).cast(DoubleType())).alias("eventos"),
            F.mean(F.col(col_verdad).cast(DoubleType())).alias("tasa"),
        )
        .withColumn("lift", F.col("tasa") / F.lit(prevalencia))
        .orderBy("decil")
    )


def puntos_roc_pr(
    df: DataFrame,
    col_verdad: str,
    col_prob: str,
    n_cuantiles: int = 100,
) -> DataFrame:
    """
    Puntos de la curva ROC y PR calculados por cuantiles del puntaje predicho.

    Devuelve un DataFrame con columnas [umbral, tpr, fpr, precision, recall].
    """
    from pyspark.sql.window import Window

    cuantiles = df.approxQuantile(col_prob, [i / n_cuantiles for i in range(n_cuantiles + 1)], 0.01)
    cuantiles = sorted(set(cuantiles), reverse=True)

    n_pos = df.filter(F.col(col_verdad) == 1).count()
    n_neg = df.filter(F.col(col_verdad) == 0).count()

    spark = df.sparkSession
    filas = []
    for umbral in cuantiles:
        agg = df.agg(
            F.sum(F.when((F.col(col_prob) >= umbral) & (F.col(col_verdad) == 1), 1).otherwise(0)).alias("tp"),
            F.sum(F.when((F.col(col_prob) >= umbral) & (F.col(col_verdad) == 0), 1).otherwise(0)).alias("fp"),
        ).collect()[0]
        tp = agg["tp"] or 0
        fp = agg["fp"] or 0
        tpr = tp / n_pos if n_pos > 0 else 0.0
        fpr = fp / n_neg if n_neg > 0 else 0.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 1.0
        recall = tpr
        filas.append({
            "umbral": float(umbral),
            "tpr": float(tpr),
            "fpr": float(fpr),
            "precision": float(precision),
            "recall": float(recall),
        })

    return spark.createDataFrame(filas, schema=["umbral", "tpr", "fpr", "precision", "recall"])


def metricas_umbral(
    df: DataFrame,
    col_verdad: str,
    col_prob: str,
    umbral: float,
) -> DataFrame:
    """
    Precisión, recall y cantidad de positivos predichos para un umbral dado.

    Devuelve un DataFrame de una fila con columnas
    [umbral, tp, fp, fn, precision, recall, n_positivos].
    """
    spark = df.sparkSession
    agg = df.agg(
        F.sum(F.when((F.col(col_prob) >= umbral) & (F.col(col_verdad) == 1), 1).otherwise(0)).alias("tp"),
        F.sum(F.when((F.col(col_prob) >= umbral) & (F.col(col_verdad) == 0), 1).otherwise(0)).alias("fp"),
        F.sum(F.when((F.col(col_prob) < umbral) & (F.col(col_verdad) == 1), 1).otherwise(0)).alias("fn"),
    ).collect()[0]
    tp, fp, fn = agg["tp"] or 0, agg["fp"] or 0, agg["fn"] or 0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    return spark.createDataFrame(
        [{"umbral": umbral, "tp": tp, "fp": fp, "fn": fn,
          "precision": precision, "recall": recall, "n_positivos": tp + fp}],
        schema=["umbral", "tp", "fp", "fn", "precision", "recall", "n_positivos"],
    )
