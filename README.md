# Etapa 2 — Infraestructura y exploración de datos

## Arquitectura

Se implementa un **Data Lakehouse** con patrón **Medallion** sobre herramientas de procesamiento de datos masivos:

```
PostgreSQL (Etapa 1)
       │
       │  JDBC (PySpark)
       ▼
  RustFS / S3
  ├── bronze/   ← copia fiel de la fuente en Parquet
  ├── silver/   ← limpieza y variables derivadas       (Etapa 2, próxima fase)
  └── gold/     ← dataset analítico para KPIs y modelo (Etapa 3)
       │
       │  TVF s3()
       ▼
  Apache Doris   ← base de datos analítica MPP
       │
       │  MySQL protocol
       ▼
  Apache Superset ← BI / Dashboard                     (Etapa 4)
```

PySpark corre en modo `local[*]` dentro del contenedor Jupyter. RustFS es el object storage S3-compatible que actúa como lake. Doris y Superset no son necesarios para correr las notebooks de esta etapa.

---

## Prerequisitos

### 1. Stack de Etapa 1 levantado con datos cargados

Este stack necesita acceso a PostgreSQL con la base `olist_db` y el esquema `olist` completo ya creado y cargado. El stack de Etapa 1 debe estar corriendo antes de ejecutar cualquier notebook.

```bash
# Desde la raíz del proyecto: solo PostgreSQL
docker compose up -d postgres
docker compose stop jupyter   # el Jupyter de Etapa 1 usa el mismo puerto (8889) que el de Etapa 2
```

> Si se levanta todo el stack de Etapa 1 (`docker compose up -d` o `make up`), su Jupyter ocupa el
> puerto 8889 y el Jupyter de Etapa 2 no puede arrancar. Alcanza con que corra `postgres`.

Verificar que PostgreSQL responde:

```bash
docker compose exec postgres psql -U olist_admin -d olist_db -c "\dt olist.*"
```

Se deben ver las 10 tablas: `orders`, `order_items`, `order_payments`, `order_reviews`, `customers`, `sellers`, `products`, `category_translation`, `geo_cep`, `geolocation`.

### 2. Variables de entorno

Copiar el archivo de ejemplo de **esta carpeta** (`etapa2/.env.example`) y completar las credenciales:

```bash
cd etapa2
cp .env.example .env
```

`POSTGRES_DB`, `POSTGRES_USER` y `POSTGRES_PASSWORD` deben ser **los mismos del `.env` de la raíz**
(los de Etapa 1); la contraseña del ejemplo es solo ilustrativa. Para comprobar que coinciden sin
mostrarla (los dos hashes deben ser iguales):

```bash
grep '^POSTGRES_PASSWORD=' .env    | cut -d= -f2- | shasum
grep '^POSTGRES_PASSWORD=' ../.env | cut -d= -f2- | shasum
```

Las variables necesarias para este stack son:

| Variable | Descripción |
|---|---|
| `RUSTFS_ACCESS_KEY` | Clave de acceso para RustFS (elegir libremente) |
| `RUSTFS_SECRET_KEY` | Clave secreta para RustFS (mínimo 8 caracteres) |
| `POSTGRES_DB` | Nombre de la base (`olist_db`) |
| `POSTGRES_USER` | Usuario de PostgreSQL (`olist_admin`) |
| `POSTGRES_PASSWORD` | Contraseña del usuario |

### 3. Jars de Spark

Ejecutar el script de descarga **una sola vez** antes de levantar el stack:

```bash
bash scripts/download_jars.sh
```

Funciona en Linux y en macOS (bash 3.2 incluido) y requiere `curl`. El script baja los 4 jars
necesarios a `jars/` desde Maven Central:

| Jar | Función |
|---|---|
| `hadoop-aws-3.4.2.jar` | Conector S3A para Hadoop 3.4.x |
| `aws-sdk-bundle-v2-2.29.52.jar` | AWS SDK v2 (requerido por Hadoop 3.4.x) |
| `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` | Soporte Apache Iceberg |
| `postgresql-42.7.4.jar` | Driver JDBC para PostgreSQL |

Si los jars ya existen el script los detecta y los omite (idempotente). El directorio `jars/` se monta directamente en los contenedores Spark y Jupyter. Los jars están en `.gitignore` y no se versiona su contenido.

---

## Levantar el stack

```bash
cd etapa2

# Primera vez: construir la imagen de Jupyter
docker compose build jupyter

# Levantar todos los servicios
docker compose up -d
```

Servicios que se levantan:

| Servicio | Puerto host | Descripción |
|---|---|---|
| `rustfs` | 9002 (API S3), 9003 (consola web) | Object storage |
| `spark-master` | 8083 (UI), 7078 | Coordinador del cluster |
| `spark-worker` | 8084 (UI) | Nodo de cómputo |
| `jupyter` | 8889 (JupyterLab), 4042 (Spark UI) | Notebooks |

Verificar que todo levantó:

```bash
docker compose ps
```

---

## Ejecutar las notebooks

Las notebooks deben correrse **en orden**. Están en `notebooks/`.

### Paso 1 — ETL Bronze: `etapa2_bronze_etl.ipynb`

Lee las 10 tablas del esquema `olist` desde PostgreSQL y las escribe como Parquet en `s3a://olist/bronze/`.

Abrir en JupyterLab: http://localhost:8889

O ejecutar sin interfaz:

```bash
docker compose exec -T jupyter bash -c "
  cd /home/notebooks &&
  jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.timeout=600 \
    etapa2_bronze_etl.ipynb
"
```

Al finalizar, la celda de verificación debe mostrar `✓` para las 10 tablas y un total de ~58 MB en el bucket.

### Paso 2 — EDA: `etapa2_eda.ipynb`

Lee los Parquet de la capa bronze directamente desde RustFS con PySpark y realiza el análisis exploratorio.

```bash
docker compose exec -T jupyter bash -c "
  cd /home/notebooks &&
  jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.timeout=300 \
    etapa2_eda.ipynb
"
```

> **Prerequisito:** el bucket `olist/bronze/` debe existir (paso 1 completado).

---

## Notas importantes

**Versiones:** la imagen de Jupyter instala `pyspark==4.1.3` para que coincida con el JVM de la imagen base `apache/spark:4.1.3-python3`. No actualizar pyspark vía pip sin actualizar también la imagen base; el mismatch de versiones rompe el API de plotting nativo y otras llamadas Py4J.

**Red:** el contenedor Jupyter está conectado a dos redes: `etapa2_net` (acceso a RustFS y Spark) y `tp-olist_olist_net` (acceso a PostgreSQL de Etapa 1 por nombre de servicio `postgres`). El stack de Etapa 1 debe estar corriendo para que esa red exista.

**Persistencia:** los datos del lake (Parquet en RustFS) se guardan en el volumen Docker `etapa2_rustfs_etapa2`. Sobreviven a `docker compose down`. Solo se pierden con `docker compose down -v`.

**Plotting en PySpark:** la API nativa `df.plot.bar(...)` usa plotly como backend. Requiere que plotly esté instalado (incluido en `requirements.txt`) y que las versiones de pyspark Python y JVM coincidan.

**Idempotencia:** el ETL bronze usa `mode('overwrite')`, por lo que puede correrse múltiples veces sin problema.

---

## Bajar el stack

```bash
docker compose down          # detiene y elimina contenedores, conserva el volumen de RustFS
docker compose down -v       # también elimina el volumen (borra todos los Parquet del lake)
```
