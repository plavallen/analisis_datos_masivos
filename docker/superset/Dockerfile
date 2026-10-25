# Imagen de Superset para el stack de etapa2: la oficial + driver para Doris.
# Reemplaza la imagen local poc-superset:local en máquinas que no tienen el poc-stack.
#
# Construir (desde la raíz del repo):
#   docker build -t poc-superset:local etapa2/docker/superset
#
# Otra versión: --build-arg SUPERSET_VERSION=6.0.0
ARG SUPERSET_VERSION=6.1.0
FROM apache/superset:${SUPERSET_VERSION}

USER root

# Doris habla el protocolo MySQL. pymysql es Python puro (no compila nada) y
# superset_config.py lo registra como MySQLdb para las URIs mysql+mysqldb://.
# Se instala en el venv de Superset (/app/.venv); se prueba uv, pip del venv y pip del sistema.
RUN (uv pip install --no-cache --python /app/.venv/bin/python pymysql \
     || /app/.venv/bin/python -m pip install --no-cache-dir pymysql \
     || pip3 install --no-cache-dir --target "$(/app/.venv/bin/python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')" pymysql) \
 && /app/.venv/bin/python -c "import pymysql; print('pymysql', pymysql.__version__)"

USER superset
