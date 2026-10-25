# Makefile — TP Integrador Olist (tp-olist)
# Todos los targets invocan docker compose. Requiere .env en el directorio raíz.
# Uso: make <target>

-include .env
-include etapa2/.env
export

COMPOSE       = docker compose -f docker-compose.yml
COMPOSE_ETAPA2 = docker compose -f etapa2/docker-compose.yml

# Carpeta de salida de mediciones. Por defecto ./mediciones (resultados de referencia
# versionados). Se puede redirigir fuera del repo con MEDICIONES_DIR en .env.
# Nota: conservar el "./" — make exporta la variable a docker compose, y sin "./"
# compose interpretaría "mediciones" como un volumen con nombre.
MEDICIONES_DIR ?= ./mediciones

# psql dentro del contenedor postgres, con ON_ERROR_STOP para detectar fallos.
PSQL     = $(COMPOSE) exec -T postgres \
           psql -v ON_ERROR_STOP=1 -U $(POSTGRES_USER) -d $(POSTGRES_DB)

.PHONY: all up down ps logs tools build-tools \
        init raw olist escala bench bench-run graficos pev publicar \
        entorno report slides clean \
        bronze silver gold tests diccionario doris referencia_kpis verificar \
        dashboard-importar dashboard-exportar riesgo doris-riesgo

# Si no existe .env, se crea desde .env.example (revisar contraseña y opcionales).
.env:
	cp .env.example .env
	@echo "Creado .env desde .env.example — revisar POSTGRES_PASSWORD y DATA_DIR."

# ── Ciclo de vida ─────────────────────────────────────────────────────────────

up: .env
	$(COMPOSE) up -d postgres jupyter

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

# Levanta el contenedor tools (pid: host). Requiere confirmación manual.
tools: .env build-tools
	$(COMPOSE) --profile tools up -d tools
	@echo "Contenedor tools levantado. Usar: docker compose exec tools bash"

build-tools:
	$(COMPOSE) build tools

# ── Etapas de carga y generación ──────────────────────────────────────────────

# Inicializa extensiones, esquemas y rol de solo lectura.
init: up
	$(PSQL) -f /sql/00_init/010_extensions_schemas_roles.sql
	@echo "Init OK"

# Carga CSVs en esquema raw.
raw: init
	$(PSQL) -f /sql/01_raw/010_ddl_raw.sql
	$(PSQL) -f /sql/01_raw/020_load_raw.sql
	@echo "raw OK"

# Transforma raw → olist (tipado, PK/FK, COMMENT ON).
olist: raw
	$(PSQL) -f /sql/02_olist/010_ddl_olist.sql
	$(PSQL) -f /sql/02_olist/020_transform_raw_olist.sql
	$(PSQL) -f /sql/02_olist/030_constraints.sql
	$(PSQL) -f /sql/02_olist/040_comments.sql
	@for t in geo_cep geolocation customers sellers category_translation \
	          products orders order_items order_payments order_reviews; do \
	    $(PSQL) -c "VACUUM ANALYZE olist.$$t;" ; \
	done
	@echo "olist OK"

# Genera esquema sintético escala (×10 por defecto).
# RESTRICCIÓN: solo para benchmarks. No usar para análisis, KPIs ni modelos.
ESCALA_FACTOR ?= 10
escala: olist
	$(PSQL) -f /sql/03_escala/010_ddl_escala.sql
	$(PSQL) -f /sql/03_escala/020_load_dimensions.sql
	$(PSQL) -v factor=$(ESCALA_FACTOR) -f /sql/03_escala/030_gen_facts.sql
	$(PSQL) -f /sql/03_escala/035_constraints_escala.sql
	@for t in orders order_items customers order_payments order_reviews \
	          geo_cep geolocation sellers category_translation products; do \
	    $(PSQL) -c "VACUUM ANALYZE escala.$$t;" ; \
	done
	$(COMPOSE) exec -T jupyter \
	    python3 /repo/scripts/validar_escala.py
	@echo "escala OK (factor=$(ESCALA_FACTOR))"

# Ejecuta benchmark (desde jupyter).
bench: escala
	$(COMPOSE) exec -T jupyter \
	    python3 /repo/scripts/benchmark.py

# Ejecuta solo el benchmark, sin recargar la base (requiere `make escala` previo).
bench-run:
	$(COMPOSE) exec -T jupyter \
	    python3 /repo/scripts/benchmark.py

# Regenera los gráficos PNG desde benchmark_resumen.csv.
graficos:
	$(COMPOSE) exec -T jupyter \
	    python3 /repo/scripts/graficar_benchmark.py

# Genera los HTML de planes (PEV) en mediciones/etapa1/planes_html/.
pev:
	$(COMPOSE) exec -T jupyter \
	    python3 /repo/scripts/generar_pev_html.py

# Copia gráficos y HTML de planes a los slides.
# Siempre desde ./mediciones (resultados de referencia), aunque MEDICIONES_DIR
# esté definida: las corridas personales nunca llegan a la presentación.
PUBLICAR_ORIGEN = mediciones/etapa1
PUBLICAR_DESTINO = slides/public/etapa1
publicar:
	@mkdir -p $(PUBLICAR_DESTINO)
	@ls $(PUBLICAR_ORIGEN)/graficos/bench_*.png >/dev/null 2>&1 && \
	    cp -v $(PUBLICAR_ORIGEN)/graficos/bench_*.png $(PUBLICAR_DESTINO)/ || \
	    echo "AVISO: no hay PNG en $(PUBLICAR_ORIGEN)/graficos/ (correr make graficos)"
	@ls $(PUBLICAR_ORIGEN)/planes_html/plan_*.html >/dev/null 2>&1 && \
	    cp -v $(PUBLICAR_ORIGEN)/planes_html/plan_*.html $(PUBLICAR_DESTINO)/ || \
	    echo "AVISO: no hay HTML en $(PUBLICAR_ORIGEN)/planes_html/ (correr make pev)"

# ── Etapa 2: pipeline analítico ───────────────────────────────────────────────

# Comando spark-submit dentro del contenedor jupyter (tiene PySpark).
ICEBERG_JARS = /opt/spark/extra-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar,/opt/spark/extra-jars/hadoop-aws-3.4.2.jar,/opt/spark/extra-jars/aws-sdk-bundle-v2-2.29.52.jar,/opt/spark/extra-jars/postgresql-42.7.4.jar
SPARK_SUBMIT = $(COMPOSE_ETAPA2) exec -T -e PYTHONPATH=/repo jupyter spark-submit --jars $(ICEBERG_JARS)

# Variables para la conexión a Doris FE (protocolo MySQL).
# Sobrescribir en .env si Doris corre en otro host/puerto.
DORIS_HOST ?= localhost
DORIS_PORT ?= 9030
DORIS_USER ?= root
DORIS_PASS ?=

# Carga las 10 tablas de olist a Parquet en bronze (RustFS).
bronze:
	$(COMPOSE_ETAPA2) exec -T jupyter python3 /repo/etapa2/notebooks/load_bronze.py

# Construye las tablas Iceberg en lake.silver desde bronze.
silver:
	$(SPARK_SUBMIT) /repo/src/pipeline/silver.py

# Construye las tablas Iceberg en lake.gold desde silver.
gold:
	$(SPARK_SUBMIT) /repo/src/pipeline/gold.py

# Ejecuta el conjunto de pruebas de calidad sobre silver y gold.
tests:
	$(SPARK_SUBMIT) /repo/src/pipeline/pruebas.py

# Regenera docs/diccionario_datos.md desde docs/diccionario.yaml.
diccionario:
	$(COMPOSE_ETAPA2) exec -T jupyter python3 /repo/docs/generar_diccionario.py

# MySQL dentro del contenedor Doris (el FE escucha en 127.0.0.1:9030 dentro del contenedor).
DORIS_MYSQL = docker exec -i etapa2-doris mysql -h127.0.0.1 -P9030 -uroot
doris:
	@set -a; [ -f etapa2/.env ] && . etapa2/.env; set +a; \
	for f in sql/doris/010_catalogo.sql sql/doris/020_base.sql \
	           sql/doris/030_vistas.sql sql/doris/040_usuario.sql; do \
	    echo "Aplicando $$f ..."; \
	    envsubst < $$f | $(DORIS_MYSQL) || exit 1; \
	done && \
	echo "Refrescando catálogo Iceberg en Doris ..." && \
	$(DORIS_MYSQL) -e "REFRESH CATALOG lake;" && \
	echo "doris OK"

# ── Predicción de riesgo (Etapa 3) ────────────────────────────────────────────
# Requisito: modelo y checkpoint de features en RustFS (s3a://olist/models/...,
# s3a://olist/checkpoints/df_features), por entrenamiento o cargados desde el export.

# Scoring: escribe lake.gold.riesgo_pedidos con el riesgo de los pedidos en curso.
riesgo:
	$(COMPOSE_ETAPA2) exec -T jupyter python3 /repo/etapa2/scripts/riesgo_pedidos.py

# Vista olist_bi.riesgo_pedidos para Superset. Requisito: make doris riesgo.
doris-riesgo:
	$(DORIS_MYSQL) -e "REFRESH CATALOG lake;"
	$(DORIS_MYSQL) < sql/doris/050_riesgo_pedidos.sql
	@echo "doris-riesgo OK"

# Variables para Superset.
SUPERSET_URL ?= http://localhost:8088

# Genera referencia_kpis.csv (KPIs calculados desde gold) para que make verificar
# compare contra el dashboard. Requisito: make gold.
referencia_kpis:
	$(COMPOSE_ETAPA2) exec -T -e PYTHONPATH=/repo \
	    -e OUTPUT_PATH=/home/notebooks/referencia_kpis.csv \
	    jupyter spark-submit --jars $(ICEBERG_JARS) /repo/scripts/gen_referencia_kpis.py
	@mkdir -p mediciones/etapa2/eda
	$(COMPOSE_ETAPA2) cp jupyter:/home/notebooks/referencia_kpis.csv mediciones/etapa2/eda/referencia_kpis.csv
	@echo "referencia_kpis.csv → mediciones/etapa2/eda/"

# Verifica el dashboard: anchos de filas, claves de mapas y KPIs contra referencia.
# Requisito: Superset y Doris levantados (make doris dashboard-importar) y
# make referencia_kpis.
verificar:
	python3 scripts/verificar_dashboard.py

# ── Dashboard: export versionado ──────────────────────────────────────────────
# Fuente de verdad: superset/olist_dashboard/ (YAML); el zip se genera desde ahí.
# Flujo de cambios: editar en la interfaz de Superset → make dashboard-exportar →
# revisar el diff en git. Para cargarlo en un Superset nuevo: make dashboard-importar.
# Requisito: contenedor etapa2-superset levantado (monta el repo en /repo).
DASHBOARD_DIR      = superset/olist_dashboard
DASHBOARD_ZIP      = superset/olist_dashboard.zip
DASHBOARD_TMP      = superset/.olist_dashboard_export.zip
SUPERSET_CONTAINER = etapa2-superset

# Empaca la carpeta YAML y la importa sobrescribiendo conexión, datasets, gráficos y dashboard.
# (superset import-dashboards no sirve: conserva sin cambios los gráficos y datasets que ya existen.)
dashboard-importar:
	python3 scripts/superset_bundle.py empacar $(DASHBOARD_DIR) $(DASHBOARD_ZIP)
	python3 scripts/superset_importar.py $(DASHBOARD_ZIP)
	@echo "Dashboard importado desde $(DASHBOARD_DIR)/"

# Exporta desde Superset y reemplaza la carpeta YAML; regenera el zip desde ella.
dashboard-exportar:
	docker exec $(SUPERSET_CONTAINER) superset export-dashboards -f /tmp/olist_dashboard_export.zip
	docker cp $(SUPERSET_CONTAINER):/tmp/olist_dashboard_export.zip $(DASHBOARD_TMP)
	python3 scripts/superset_bundle.py desempacar $(DASHBOARD_TMP) $(DASHBOARD_DIR)
	python3 scripts/superset_bundle.py empacar $(DASHBOARD_DIR) $(DASHBOARD_ZIP)
	@rm -f $(DASHBOARD_TMP)
	@echo "Dashboard exportado → $(DASHBOARD_DIR)/ (revisar con: git diff --stat $(DASHBOARD_DIR))"

# Reconstrucción completa desde cero.
all: up init raw olist escala bench entorno

# ── Mediciones del entorno ────────────────────────────────────────────────────
# Funciona en Linux y macOS. En macOS los contenedores corren dentro de la VM de
# Docker Desktop, por eso además se registra CPU/memoria vistos desde el contenedor.

entorno:
	@mkdir -p $(MEDICIONES_DIR)/etapa1/entorno
	@if [ "$$(uname -s)" = "Darwin" ]; then \
	    sysctl -n machdep.cpu.brand_string hw.ncpu hw.physicalcpu \
	        > $(MEDICIONES_DIR)/etapa1/entorno/lscpu.txt ; \
	    sysctl -n hw.memsize | awk '{printf "host_mem_total: %.1f GiB\n", $$1/1073741824}' \
	        > $(MEDICIONES_DIR)/etapa1/entorno/memoria.txt ; \
	    diskutil info / > $(MEDICIONES_DIR)/etapa1/entorno/disco.txt 2>&1 || true ; \
	else \
	    lscpu   > $(MEDICIONES_DIR)/etapa1/entorno/lscpu.txt ; \
	    free -h > $(MEDICIONES_DIR)/etapa1/entorno/memoria.txt ; \
	    lsblk -d -o NAME,ROTA,SIZE,MODEL > $(MEDICIONES_DIR)/etapa1/entorno/disco.txt ; \
	fi
	df -h    > $(MEDICIONES_DIR)/etapa1/entorno/df.txt
	$(COMPOSE) exec -T postgres sh -c 'uname -a; nproc; grep -E "MemTotal|MemAvailable" /proc/meminfo' \
	    > $(MEDICIONES_DIR)/etapa1/entorno/contenedor.txt
	docker info    > $(MEDICIONES_DIR)/etapa1/entorno/docker_info.txt
	docker version > $(MEDICIONES_DIR)/etapa1/entorno/docker_version.txt
	$(COMPOSE) exec -T postgres \
	    psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	    "SELECT name, setting, unit FROM pg_settings \
	     WHERE name IN ('shared_buffers','work_mem','max_connections', \
	                    'track_io_timing','shared_preload_libraries', \
	                    'pg_stat_statements.track') \
	     ORDER BY name;" \
	    > $(MEDICIONES_DIR)/etapa1/entorno/pg_settings.txt
	$(COMPOSE) exec -T postgres pg_test_timing \
	    > $(MEDICIONES_DIR)/etapa1/entorno/pg_test_timing.txt 2>&1 || true
	@echo "Entorno registrado en $(MEDICIONES_DIR)/etapa1/entorno/"

# ── Presentación e informe ────────────────────────────────────────────────────

slides:
	cd slides && npx slidev export etapa1.md --output public/etapa1/etapa1.pdf

report:
	@command -v pandoc >/dev/null 2>&1 && \
	    pandoc informes/etapa1/informe.md -o informes/etapa1/informe.pdf || \
	    echo "pandoc no disponible; informe en Markdown: informes/etapa1/informe.md"

# ── Limpieza ──────────────────────────────────────────────────────────────────

clean:
	$(COMPOSE) down -v
	@echo "Contenedores y volumen pgdata eliminados."
