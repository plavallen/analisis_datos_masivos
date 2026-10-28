# TP Integrador Olist — Logística de entregas

Pipeline completo sobre el dataset público de Olist: carga en PostgreSQL, lake con patrón
Medallion (bronze, silver, gold) en RustFS con Spark e Iceberg, capa analítica en Apache Doris,
modelo de riesgo de atraso y dashboard en Apache Superset.

Todo corre en contenedores Docker y se ejecuta desde la raíz del repositorio con `make`.

## Arquitectura

```
CSV de Olist (data/raw/)
       │  COPY
       ▼
PostgreSQL           ← Etapa 1: esquemas raw y olist (tipado, PK/FK)
       │  JDBC (PySpark)
       ▼
RustFS / S3 (lake)
  ├── bronze/        ← copia fiel de PostgreSQL en Parquet
  ├── silver/        ← limpieza y variables derivadas (Iceberg)
  ├── gold/          ← tablas analíticas para KPIs y modelo (Iceberg)
  └── models/        ← modelo de riesgo (Spark ML) + checkpoint de features
       │  catálogo Iceberg
       ▼
Apache Doris         ← base analítica olist_bi (vistas sobre gold)
       │  protocolo MySQL
       ▼
Apache Superset      ← dashboard "OLIST Logística"
```

Spark corre como driver dentro del contenedor `jupyter` (más un cluster `spark-master` y
`spark-worker`). MLflow registra los experimentos del modelo.

---

## 1. Requisitos

- **Docker** con Docker Compose v2 (`docker compose version`): Docker Desktop en macOS (Intel o
  Apple Silicon) o Docker Engine en Linux.
- **Memoria asignada a Docker: 12 GB o más** (Docker Desktop → Settings → Resources → Memory).
  Doris, Spark y Superset corren al mismo tiempo.
- **Disco libre: ~15 GB** (imágenes, jars y volúmenes).
- En el host: `make`, `bash`, `curl` y `python3` (solo biblioteca estándar; se usa para
  importar el dashboard). macOS y la mayoría de las distribuciones Linux ya los traen.
- Conexión a internet la primera vez (descarga de imágenes y jars).
- Puertos libres: 5432, 8030, 8083, 8084, 8088, 8889, 9002, 9003, 9030, 4042, 5050, 7078.

## 2. Datos crudos

Copiar los 9 CSV del dataset *Brazilian E-Commerce Public Dataset by Olist*
(https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) en `data/raw/`, sin renombrarlos:

```
data/raw/
├── olist_customers_dataset.csv
├── olist_geolocation_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_orders_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
└── product_category_name_translation.csv
```

Para comprobarlo: `make verificar-csv`.

---

## 3. Ejecución completa (un solo comando)

```bash
make todo
```

Hace, en orden: crea `.env` desde `.env.example`, verifica los CSV, descarga los jars de Spark,
construye las imágenes, levanta el stack, carga PostgreSQL, construye bronze, silver y gold,
configura Doris, entrena el modelo, calcula el riesgo de los pedidos en curso e importa el
dashboard.

Duración aproximada la primera vez: **1 a 1.5 horas**. La mayor parte es la notebook de
entrenamiento (~40 min). Si se tiene el export del modelo ya entrenado, ver la sección 4.5,
opción B, para saltar ese paso.

> **Mensajes de error que no detienen el proceso.** Durante la ejecución, en especial en la
> predicción (sección 4.5), Spark imprime muchos avisos `WARN`, algunos con un stack trace de
> Java. Son normales y **hay que dejar que el proceso siga**; no cancelarlo. El proceso solo
> falló si `make` termina con `make: *** [...] Error 1`. En ese caso se puede retomar desde el
> paso que falló (sección 4) sin repetir los anteriores.

Al terminar, abrir **http://localhost:8088** (usuario `admin`, contraseña `admin`) y el
dashboard **OLIST Logística**.

---

## 4. Ejecución paso a paso

Cada paso se puede correr por separado y volver a correr sin romper nada (idempotente).

### 4.1 Levantar el stack

```bash
make up
```

Crea `.env` si no existe, verifica los CSV, descarga los jars a `jars/`, construye las imágenes
`olist-jupyter:local` y `olist-superset:local` y levanta todos los servicios. La primera vez
tarda varios minutos: Superset espera a que Doris esté sano (~3 min).

Comprobar con `make ps`: todos los servicios deben estar `running` (Doris y PostgreSQL además
`healthy`); `olist-rustfs-init` termina solo después de crear el bucket `olist`.

### 4.2 Etapa 1: carga en PostgreSQL

```bash
make olist        # init → raw → olist
make verificar-raw    # opcional: conteos por tabla
```

- `init`: extensión `pg_stat_statements`, esquemas, rol de solo lectura y base `mlflow_db`.
- `raw`: carga los 9 CSV tal cual en el esquema `raw`.
- `olist`: esquema tipado con PK/FK y comentarios (10 tablas, incluye `geo_cep`).

### 4.3 Etapa 2: lake (bronze, silver, gold)

```bash
make bronze       # PostgreSQL → Parquet en s3a://olist/bronze/
make silver       # limpieza → tablas Iceberg lake.silver
make gold         # tablas analíticas → lake.gold
make tests        # opcional: pruebas de calidad sobre silver y gold
```

### 4.4 Doris

```bash
make doris
```

Crea el catálogo Iceberg `lake` sobre RustFS, la base `olist_bi` con sus vistas y el usuario de
solo lectura para Superset.

### 4.5 Etapa 3: modelo de riesgo de atraso

**Opción A — entrenar desde cero** (lo que hace `make todo`):

```bash
make prediccion
```

Equivale a:

| Paso | Qué hace | Duración |
|---|---|---|
| `make modelo-features` | Ejecuta `notebooks/etapa3/01_regresion_logistica.ipynb`: features, búsqueda de hiperparámetros, registro en MLflow y checkpoint `s3a://olist/checkpoints/df_features`. Deja la copia ejecutada en `notebooks/etapa3/01_regresion_logistica_ejecutado.ipynb`. | ~40 min |
| `make modelo` | Entrena el modelo final y lo guarda en `s3a://olist/models/olist_tarde_logistica_v1`. | ~5 min |
| `make riesgo` | Calcula el riesgo de los pedidos en curso al corte simulado (15/06/2018) → `lake.gold.riesgo_pedidos`. | ~2 min |
| `make doris-riesgo` | Crea la vista `olist_bi.riesgo_pedidos` para el dashboard. | segundos |

> **Proceso largo y pesado.** La predicción es la etapa que más tiempo y cómputo consume:
> calcula variables con ventanas sobre todo el historial de vendedores y ajusta decenas de
> modelos en la búsqueda de hiperparámetros. Puede tardar de 30 minutos a más de una hora según
> la máquina, con el CPU al máximo y varios GB de memoria ocupados. Conviene no correr otras
> tareas pesadas mientras tanto y verificar que Docker tenga la memoria indicada en la sección 1.
> Durante largos ratos la consola solo muestra el avance de los stages de Spark
> (`[Stage 112:> (0 + 2) / 2]`); es normal. El avance se puede seguir en la UI de Spark
> (http://localhost:4042) y en MLflow (http://localhost:5050).
>
> **Avisos esperados en esta etapa (no cancelar):**
> - `FileNotFoundException: No such file or directory: s3a://olist/checkpoints/df_features`:
>   la primera vez el checkpoint de features todavía no existe; la notebook lo detecta, calcula
>   las features desde cero y lo guarda. En las siguientes ejecuciones lo reutiliza y salta ese
>   cálculo.
> - `QuantileDiscretizer: Some quantiles were identical`, `Failed to load implementation from:
>   dev.ludovic.netlib.blas.JNIBLAS` y `Using standard FileOutputCommitter`: avisos
>   informativos de Spark, sin efecto en el resultado.
>
> Si la notebook falla con `OutOfMemoryError`, subir la memoria de Docker y volver a correr
> `make prediccion`: como el checkpoint ya quedó guardado, la segunda vez es mucho más rápida.

**Opción B — usar el modelo ya entrenado** (archivo `prediccion_export.tar.gz`):

```bash
make prediccion-rapida PREDICCION_EXPORT=/ruta/a/prediccion_export.tar.gz
```

Carga el checkpoint y el modelo en RustFS y después corre `riesgo` y `doris-riesgo`. Si el
archivo está en la raíz del repo no hace falta indicar `PREDICCION_EXPORT`.

### 4.6 Dashboard

```bash
make dashboard-importar
```

Importa conexión, datasets, gráficos y dashboard desde `superset/olist_dashboard/` (YAML).
Abrir http://localhost:8088 → Dashboards → **OLIST Logística**. Pestañas: Vista ejecutiva,
Vista analítica, Vista de decisión, Vista predictiva, entre otras.

La pestaña **Vista predictiva** necesita el paso 4.5; sin él sus gráficos muestran error.

---

## 5. Servicios

| Servicio | URL / puerto | Uso |
|---|---|---|
| Superset | http://localhost:8088 | Dashboard (admin / admin) |
| JupyterLab | http://localhost:8889 | Notebooks (`notebooks/`) |
| MLflow | http://localhost:5050 | Experimentos del modelo |
| Consola RustFS | http://localhost:9003 | Explorar el lake (credenciales `RUSTFS_*` de `.env`) |
| Spark master UI | http://localhost:8083 | Estado del cluster |
| Doris (MySQL) | localhost:9030 | `mysql -h 127.0.0.1 -P 9030 -uroot` |
| PostgreSQL | localhost:5432 | Usuario y contraseña `POSTGRES_*` de `.env` |

## 6. Detener y reiniciar

```bash
make down     # detiene los contenedores; los datos se conservan en los volúmenes
make up       # vuelve a levantar; no hace falta repetir la carga
make clean    # borra contenedores y TODOS los volúmenes (PostgreSQL, lake, Doris, Superset)
```

Después de `make clean` hay que repetir todo desde la sección 3 o 4.

## 7. Estructura del repositorio

```
docker-compose.yml        stack completo
Makefile                  todos los pasos (make <target>)
.env.example              variables de entorno (se copia a .env)
data/raw/                 CSV de Olist (no se versionan)
docker/                   Dockerfiles de jupyter y superset, arranque de MLflow
sql/00_init, 01_raw, 02_olist   carga en PostgreSQL (Etapa 1)
sql/doris/                catálogo, vistas y usuario en Doris
src/pipeline/             jobs Spark: silver, gold, pruebas
src/modelo/               features, entrenamiento y evaluación del modelo
lib/sparkstats/           utilidades estadísticas sobre PySpark (usadas por el modelo)
scripts/                  carga de jars, entrenamiento, scoring, import/export del dashboard
notebooks/                EDA (Etapa 2) y modelo (etapa3/)
superset/olist_dashboard/ dashboard en YAML (fuente de verdad)
docs/                     diccionario de datos y definición de KPIs (kpis.yaml)
```

## 8. Problemas comunes

**Doris no arranca o se queda en "wait catalog to be ready".** La primera vez tarda hasta
~3 min; ver el avance con `docker compose logs -f doris`. El compose le fija la IP para que no
cambie entre reinicios. Si aun así queda trabado, borrar solo sus volúmenes y repetir
`make doris` y los pasos siguientes:

```bash
docker compose stop doris superset
docker compose rm -f doris
docker volume rm olist_doris_fe_meta olist_doris_be_data
make up doris doris-riesgo
```

**Error de subred al levantar** (`Pool overlaps with other one on this address space`): otra red
de Docker usa `10.89.40.0/24`. Cambiar la subred y la IP de Doris en `docker-compose.yml`.

**Un puerto ya está ocupado.** Detener el programa que lo usa o cambiar el puerto del lado del
host en `docker-compose.yml` (el número a la izquierda de `:`).

**Contenedores que se reinician o Spark sin memoria.** Subir la memoria asignada a Docker
(sección 1).

**Superset muestra errores en la Vista predictiva.** Falta el paso 4.5.

**Los mapas no muestran el mapa base.** Requieren un token de Mapbox en `MAPBOX_API_KEY`
(`.env`); los datos se ven igual sin él.

## 9. Notas técnicas

- **Versiones:** la imagen de Jupyter parte de `apache/spark:4.1.3-python3` e instala
  `pyspark==4.1.3`; deben coincidir (un mismatch rompe llamadas Py4J). Los jars de Iceberg y
  S3A son para Spark 4.1 / Scala 2.13. Superset está fijo en 6.1.0, la versión con la que se
  exportó el dashboard.
- **Cambios al dashboard:** editar en Superset, luego `make dashboard-exportar` y revisar el
  diff de `superset/olist_dashboard/`.
- **Verificación del dashboard (opcional):** `make referencia_kpis` y después `make verificar`.
