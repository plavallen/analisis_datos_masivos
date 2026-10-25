from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def wilson_ci(df: DataFrame, col_n: str, col_k: str, z: float = 1.96, prefijo: str = "wilson") -> DataFrame:
    """Agrega intervalos de confianza de Wilson para una proporción k/n."""
    z2 = z ** 2
    return (
        df
        .withColumn("_p", F.col(col_k).cast("double") / F.col(col_n))
        .withColumn("_d", F.lit(1.0) + F.lit(z2) / F.col(col_n))
        .withColumn(f"{prefijo}_lo",
            (F.col("_p") + F.lit(z2) / (2 * F.col(col_n))
             - F.lit(z) * F.sqrt(
                 F.col("_p") * (1 - F.col("_p")) / F.col(col_n)
                 + F.lit(z2) / (4 * F.col(col_n) ** 2)
             )) / F.col("_d"))
        .withColumn(f"{prefijo}_hi",
            (F.col("_p") + F.lit(z2) / (2 * F.col(col_n))
             + F.lit(z) * F.sqrt(
                 F.col("_p") * (1 - F.col("_p")) / F.col(col_n)
                 + F.lit(z2) / (4 * F.col(col_n) ** 2)
             )) / F.col("_d"))
        .drop("_p", "_d")
    )


def tasa_por(df: DataFrame, group_cols, col_tarde: str = "tarde", n_min: int = 30) -> DataFrame:
    """
    Tasa de tardíos por grupo con intervalo de Wilson al 95%.
    Filtra grupos con n < n_min.
    """
    if isinstance(group_cols, str):
        group_cols = [group_cols]

    agg = (
        df.groupBy(*group_cols)
        .agg(
            F.count("*").alias("n"),
            F.sum(F.col(col_tarde).cast("int")).alias("tardios"),
        )
        .withColumn("p", F.col("tardios").cast("double") / F.col("n"))
        .filter(F.col("n") >= n_min)
    )
    return (
        wilson_ci(agg, "n", "tardios")
        .orderBy(F.desc("p"))
    )
