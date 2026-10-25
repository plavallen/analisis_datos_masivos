#!/usr/bin/env bash
# Descarga los jars necesarios para PySpark en este stack.
# Destino: etapa2/jars/  (montado en los contenedores Spark y Jupyter; ignorado por git).
# Ejecutar una sola vez antes de levantar el stack: bash scripts/download_jars.sh
# Compatible con bash 3.2 (macOS) y bash 4+ (Linux): no usa arreglos asociativos.

set -euo pipefail

DEST="$(cd "$(dirname "$0")/.." && pwd)/jars"
mkdir -p "$DEST"

MAVEN="https://repo1.maven.org/maven2"

# Una línea por jar: <nombre_local> <url>
JARS="
postgresql-42.7.4.jar                      $MAVEN/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
hadoop-aws-3.4.2.jar                       $MAVEN/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar
aws-sdk-bundle-v2-2.29.52.jar              $MAVEN/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar
iceberg-spark-runtime-4.1_2.13-1.11.0.jar  $MAVEN/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar
"

echo "Destino: $DEST"
echo ""

echo "$JARS" | while read -r JAR URL; do
  [[ -z "$JAR" ]] && continue
  if [[ -f "$DEST/$JAR" ]]; then
    echo "✓ ya existe: $JAR"
  else
    echo "↓ descargando $JAR ..."
    # Se descarga a un archivo temporal: si falla a la mitad no queda un jar incompleto
    curl -fSL --progress-bar "$URL" -o "$DEST/$JAR.part"
    mv "$DEST/$JAR.part" "$DEST/$JAR"
  fi
done

echo ""
echo "Jars disponibles en $DEST:"
ls -lh "$DEST/"*.jar
