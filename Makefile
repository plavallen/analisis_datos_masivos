# Makefile — TP Integrador Olist
# Todos los targets invocan docker compose sobre docker-compose.yml (raíz del repo).
# Requiere .env en la raíz (se crea desde .env.example) y los 9 CSV de Olist en data/raw/.
# Uso: make <target>    — reconstrucción completa desde los CSV: make todo

-include .env
export

COMPOSE = docker compose -f docker-compose.yml

# psql dentro del contenedor postgres, con ON_ERROR_STOP para detectar fallos.
PSQL = $(COMPOSE) exec -T postgres \
       psql -v ON_ERROR_STOP=1 -U $(POSTGRES_USER) -d $(POSTGRES_DB)

# MySQL dentro del contenedor Doris (el FE escucha en 127.0.0.1:9030 dentro del contenedor).
DORIS_MYSQL = $(COMPOSE) exec -T doris mysql -h127.0.0.1 -P9030 -uroot

# Sustituye ${VAR} con las variables de .env (exportadas arriba). Se usa python3 del host
# en lugar de envsubst porque macOS no trae envsubst.
EXPANDIR_VARS = python3 -c 'import os,sys; sys.stdout.write(os.path.expandvars(sys.stdin.read()))'

SUPERSET_URL ?= http://localhost:8088

CSV_OLIST = olist_customers_dataset.csv olist_geolocation_dataset.csv \
            olist_orders_dataset.csv olist_order_items_dataset.csv \
            olist_order_payments_dataset.csv olist_order_reviews_dataset.csv \
            olist_products_dataset.csv olist_sellers_dataset.csv \
            product_category_name_translation.csv

.PHONY: todo base prediccion prediccion-rapida \
        verificar-csv jars build up down ps logs clean \
        esperar-postgres esperar-doris esperar-superset \
        init raw olist verificar-raw \
        bronze silver gold tests diccionario doris \
        modelo-features modelo modelo-importar riesgo doris-riesgo \
        dashboard-importar dashboard-exportar referencia_kpis verificar

# ── Ejecución completa ────────────────────────────────────────────────────────

# Desde los CSV hasta el dashboard, entrenando el modelo (la notebook de features
# tarda ~40 min la primera vez).
todo: base prediccion dashboard-importar
	@echo "Listo: dashboard en $(SUPERSET_URL) (usuario/contraseña de .env)"

# Datos y capa analítica: PostgreSQL → bronze → silver → gold → Doris.
base: up olist bronze silver gold doris

# Predicción de riesgo entrenando el modelo desde cero.
prediccion: modelo-features modelo riesgo doris-riesgo

# Predicción de riesgo con el modelo ya entrenado (export prediccion_export.tar.gz).
prediccion-rapida: modelo-importar riesgo doris-riesgo

# ── Preparación y ciclo de vida ──────────────────────────────────────────────

# Si no existe .env, se crea desde .env.example.
.env:
	cp .env.example .env
	@echo "Creado .env desde .env.example (revisar contraseñas si se desea)."

# Comprueba que estén los 9 CSV de Olist en data/raw/.
verificar-csv:
	@faltan=0; for f in $(CSV_OLIST); do \
	    [ -f data/raw/$$f ] || { echo "FALTA data/raw/$$f"; faltan=1; }; \
	done; \
	[ $$faltan -eq 0 ] && echo "CSV OK (9 archivos en data/raw/)" || \
	{ echo "Copiar los CSV de Olist a data/raw/ (ver README)"; exit 1; }

# Descarga los jars de Spark a jars/ (idempotente).
jars:
	bash scripts/download_jars.sh

# Construye las imágenes locales (jupyter, también usada por mlflow, y superset).
build: .env
	$(COMPOSE) build jupyter superset

# Levanta todo el stack. Superset espera a que Doris esté sano (puede tardar ~3 min).
up: .env verificar-csv jars build
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

# Borra contenedores y TODOS los volúmenes (PostgreSQL, lake, Doris, Superset).
clean:
	$(COMPOSE) down -v

esperar-postgres:
	@echo "Esperando PostgreSQL ..."; n=0; \
	until $(COMPOSE) exec -T postgres pg_isready -U $(POSTGRES_USER) -d $(POSTGRES_DB) >/dev/null 2>&1; do \
	    n=$$((n+1)); [ $$n -gt 60 ] && { echo "PostgreSQL no responde"; exit 1; }; sleep 2; \
	done; echo "PostgreSQL listo"

esperar-doris:
	@echo "Esperando Doris (la primera vez tarda unos minutos) ..."; n=0; \
	until $(DORIS_MYSQL) -e "SELECT 1" >/dev/null 2>&1; do \
	    n=$$((n+1)); [ $$n -gt 120 ] && { echo "Doris no responde (ver README, sección Problemas)"; exit 1; }; sleep 5; \
	done; echo "Doris listo"

esperar-superset:
	@echo "Esperando Superset ..."; n=0; \
	until curl -sf $(SUPERSET_URL)/health >/dev/null 2>&1; do \
	    n=$$((n+1)); [ $$n -gt 120 ] && { echo "Superset no responde ($(SUPERSET_URL))"; exit 1; }; sleep 5; \
	done; echo "Superset listo"

# ── Etapa 1: carga en PostgreSQL ──────────────────────────────────────────────

# Extensiones, esquemas, rol de solo lectura y base de MLflow.
init: esperar-postgres
	$(PSQL) -f /sql/00_init/010_extensions_schemas_roles.sql
	$(PSQL) -f /sql/00_init/020_mlflow_db.sql
	@# MLflow arranca con make up antes de que exista mlflow_db; se reinicia para que la tome.
	$(COMPOSE) restart mlflow
	@echo "init OK"

# Carga los CSV en el esquema raw.
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

# Conteos del esquema raw (comprobación opcional).
verificar-raw:
	$(PSQL) -f /sql/01_raw/030_verificar_raw.sql

# ── Etapa 2: lake (bronze, silver, gold) ──────────────────────────────────────

# Comando spark-submit dentro del contenedor jupyter (tiene PySpark).
ICEBERG_JARS = /opt/spark/extra-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar,/opt/spark/extra-jars/hadoop-aws-3.4.2.jar,/opt/spark/extra-jars/aws-sdk-bundle-v2-2.29.52.jar,/opt/spark/extra-jars/postgresql-42.7.4.jar
SPARK_SUBMIT = $(COMPOSE) exec -T -e PYTHONPATH=/repo jupyter spark-submit --jars $(ICEBERG_JARS)

# Copia las 10 tablas de olist (PostgreSQL) a Parquet en bronze (RustFS).
bronze:
	$(COMPOSE) exec -T jupyter python3 /repo/notebooks/load_bronze.py

# Construye las tablas Iceberg en lake.silver desde bronze.
silver:
	$(SPARK_SUBMIT) /repo/src/pipeline/silver.py

# Construye las tablas Iceberg en lake.gold desde silver.
gold:
	$(SPARK_SUBMIT) /repo/src/pipeline/gold.py

# Pruebas de calidad sobre silver y gold (opcional).
tests:
	$(SPARK_SUBMIT) /repo/src/pipeline/pruebas.py

# Regenera docs/diccionario_datos.md desde docs/diccionario.yaml.
diccionario:
	$(COMPOSE) exec -T jupyter python3 /repo/docs/generar_diccionario.py

# Catálogo Iceberg, base olist_bi, vistas y usuario de solo lectura en Doris.
doris: esperar-doris
	@for f in sql/doris/010_catalogo.sql sql/doris/020_base.sql \
	           sql/doris/030_vistas.sql sql/doris/040_usuario.sql; do \
	    echo "Aplicando $$f ..."; \
	    $(EXPANDIR_VARS) < $$f | $(DORIS_MYSQL) || exit 1; \
	done
	@echo "Refrescando catálogo Iceberg en Doris ..."
	$(DORIS_MYSQL) -e "REFRESH CATALOG lake;"
	@echo "doris OK"

# ── Etapa 3: modelo y predicción de riesgo ────────────────────────────────────

# Notebook de entrenamiento: calcula el checkpoint de features (s3a://olist/checkpoints/df_features,
# ~40 min la primera vez) y registra los experimentos en MLflow. Requisito: make gold.
# (src/modelo/config.py guarda gráficos en rutas relativas a la carpeta de la notebook.)
modelo-features:
	@mkdir -p notebooks/etapa3/mediciones/etapa3/logistica/modelo notebooks/mediciones/etapa3/logistica
	$(COMPOSE) exec -T jupyter jupyter nbconvert --to notebook --execute \
	    --ExecutePreprocessor.timeout=-1 \
	    --output 01_regresion_logistica_ejecutado.ipynb \
	    /home/notebooks/etapa3/01_regresion_logistica.ipynb

# Entrena el modelo final y lo guarda en s3a://olist/models/olist_tarde_logistica_v1 (~5 min).
modelo:
	$(COMPOSE) exec -T jupyter python3 /repo/scripts/retrain_modelo.py

# Alternativa a modelo-features + modelo: carga a RustFS el checkpoint y el modelo ya
# entrenados desde el export (por defecto ./prediccion_export.tar.gz).
PREDICCION_EXPORT ?= prediccion_export.tar.gz
PREDICCION_TMP     = .prediccion_tmp
modelo-importar:
	@[ -f "$(PREDICCION_EXPORT)" ] || { echo "No existe $(PREDICCION_EXPORT)"; exit 1; }
	rm -rf $(PREDICCION_TMP) && mkdir -p $(PREDICCION_TMP)
	tar xzf "$(PREDICCION_EXPORT)" -C $(PREDICCION_TMP)
	$(COMPOSE) run --rm --no-deps -v "$(CURDIR)/$(PREDICCION_TMP)/prediccion_export:/export:ro" \
	    --entrypoint aws rustfs-init s3 sync /export/checkpoints/df_features/ \
	    s3://olist/checkpoints/df_features/ --endpoint-url http://rustfs:9000 --no-cli-pager
	$(COMPOSE) run --rm --no-deps -v "$(CURDIR)/$(PREDICCION_TMP)/prediccion_export:/export:ro" \
	    --entrypoint aws rustfs-init s3 sync /export/models/olist_tarde_logistica_v1/ \
	    s3://olist/models/olist_tarde_logistica_v1/ --endpoint-url http://rustfs:9000 --no-cli-pager
	rm -rf $(PREDICCION_TMP)
	@echo "modelo-importar OK"

# Scoring: escribe lake.gold.riesgo_pedidos con el riesgo de los pedidos en curso.
riesgo:
	$(COMPOSE) exec -T jupyter python3 /repo/scripts/riesgo_pedidos.py

# Vista olist_bi.riesgo_pedidos para Superset. Requisito: make doris riesgo.
doris-riesgo: esperar-doris
	$(DORIS_MYSQL) -e "REFRESH CATALOG lake;"
	$(DORIS_MYSQL) < sql/doris/050_riesgo_pedidos.sql
	@echo "doris-riesgo OK"

# ── Dashboard (Superset) ──────────────────────────────────────────────────────
# Fuente de verdad: superset/olist_dashboard/ (YAML); el zip se genera desde ahí.
# Flujo de cambios: editar en la interfaz de Superset → make dashboard-exportar →
# revisar el diff en git. Para cargarlo en un Superset nuevo: make dashboard-importar.
DASHBOARD_DIR = superset/olist_dashboard
DASHBOARD_ZIP = superset/olist_dashboard.zip
DASHBOARD_TMP = superset/.olist_dashboard_export.zip

# Empaca la carpeta YAML y la importa sobrescribiendo conexión, datasets, gráficos y dashboard.
# (superset import-dashboards no sirve: conserva sin cambios los gráficos y datasets que ya existen.)
dashboard-importar: esperar-superset
	python3 scripts/superset_bundle.py empacar $(DASHBOARD_DIR) $(DASHBOARD_ZIP)
	python3 scripts/superset_importar.py $(DASHBOARD_ZIP)
	@echo "Dashboard importado desde $(DASHBOARD_DIR)/ → $(SUPERSET_URL)"

# Exporta desde Superset y reemplaza la carpeta YAML; regenera el zip desde ella.
dashboard-exportar:
	$(COMPOSE) exec -T superset superset export-dashboards -f /tmp/olist_dashboard_export.zip
	$(COMPOSE) cp superset:/tmp/olist_dashboard_export.zip $(DASHBOARD_TMP)
	python3 scripts/superset_bundle.py desempacar $(DASHBOARD_TMP) $(DASHBOARD_DIR)
	python3 scripts/superset_bundle.py empacar $(DASHBOARD_DIR) $(DASHBOARD_ZIP)
	@rm -f $(DASHBOARD_TMP)
	@echo "Dashboard exportado → $(DASHBOARD_DIR)/ (revisar con: git diff --stat $(DASHBOARD_DIR))"

# ── Verificación (opcional) ───────────────────────────────────────────────────

# Genera referencia_kpis.csv (KPIs calculados desde gold) para que make verificar
# compare contra el dashboard. Requisito: make gold.
referencia_kpis:
	$(COMPOSE) exec -T -e PYTHONPATH=/repo \
	    -e OUTPUT_PATH=/home/notebooks/referencia_kpis.csv \
	    jupyter spark-submit --jars $(ICEBERG_JARS) /repo/scripts/gen_referencia_kpis.py
	@mkdir -p mediciones/etapa2/eda
	$(COMPOSE) cp jupyter:/home/notebooks/referencia_kpis.csv mediciones/etapa2/eda/referencia_kpis.csv
	@echo "referencia_kpis.csv → mediciones/etapa2/eda/"

# Verifica el dashboard: anchos de filas, claves de mapas y KPIs contra referencia.
# Requisito: make dashboard-importar y make referencia_kpis.
verificar:
	python3 scripts/verificar_dashboard.py
