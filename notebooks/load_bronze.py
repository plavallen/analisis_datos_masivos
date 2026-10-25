"""
	Carga los datos en la capa bronze.
	Lee datos de postgres (sistema trasaccional  de la organización) 
	y los almacena (como parquet) en nuestra nube. 
	Los datos serán consumidos desde nuestra infraestructura sin acceder al sistema transaccional.
	Separa la parte de extraccion del procesamineto para Machine Learning y  Analítica.
"""
import os
import time
from pyspark.sql import SparkSession

PG_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
PG_PORT = os.environ.get('POSTGRES_PORT', '5432')
PG_DB   = os.environ.get('POSTGRES_DB',   'olist_db')
PG_USER = os.environ.get('POSTGRES_USER', 'olist_admin')
PG_PASS = os.environ.get('POSTGRES_PASSWORD', '')

RUSTFS_ENDPOINT = os.environ.get('RUSTFS_ENDPOINT', 'http://rustfs:9000')
S3_KEY = os.environ.get('AWS_ACCESS_KEY_ID',     'rustfsadmin')
S3_SEC = os.environ.get('AWS_SECRET_ACCESS_KEY', 'rustfsadmin123')

JARS = ','.join([
    '/opt/spark/extra-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar',
    '/opt/spark/extra-jars/hadoop-aws-3.4.2.jar',
    '/opt/spark/extra-jars/aws-sdk-bundle-v2-2.29.52.jar',
    '/opt/spark/extra-jars/postgresql-42.7.4.jar',
])

spark = (
    SparkSession.builder
    .appName('load-bronze')
    .master('local[*]')
    .config('spark.jars', JARS)
    .config('spark.hadoop.fs.s3a.impl',                   'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.endpoint',               RUSTFS_ENDPOINT)
    .config('spark.hadoop.fs.s3a.access.key',             S3_KEY)
    .config('spark.hadoop.fs.s3a.secret.key',             S3_SEC)
    .config('spark.hadoop.fs.s3a.path.style.access',      'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
    .config('spark.sql.extensions',
            'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lake',           'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lake.type',      'hadoop')
    .config('spark.sql.catalog.lake.warehouse', 's3a://olist/warehouse')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('ERROR')

JDBC_URL = f'jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}'
JDBC_PROPS = {
    'user':     PG_USER,
    'password': PG_PASS,
    'driver':   'org.postgresql.Driver',
    'fetchsize': '10000',
}

TABLES = [
    ('orders',               None,             1),
    ('order_items',          None,             1),
    ('order_payments',       None,             1),
    ('order_reviews',        None,             1),
    ('customers',            None,             1),
    ('sellers',              None,             1),
    ('products',             None,             1),
    ('category_translation', None,             1),
    ('geo_cep',              None,             1),
    ('geolocation',          'geolocation_id', 4),
]

BRONZE_BASE = 's3a://olist/bronze'

for tabla, col_part, n_part in TABLES:
    t0   = time.time()
    ruta = f'{BRONZE_BASE}/{tabla}'

    if col_part:
        bounds = spark.read.jdbc(
            url=JDBC_URL,
            table=f'(SELECT MIN({col_part}), MAX({col_part}) FROM olist.{tabla}) t',
            properties=JDBC_PROPS,
        ).collect()[0]
        lo, hi = int(bounds[0]), int(bounds[1])
        df = spark.read.jdbc(
            url=JDBC_URL,
            table=f'olist.{tabla}',
            column=col_part,
            lowerBound=lo,
            upperBound=hi,
            numPartitions=n_part,
            properties=JDBC_PROPS,
        )
    else:
        df = spark.read.jdbc(
            url=JDBC_URL,
            table=f'olist.{tabla}',
            properties=JDBC_PROPS,
        )

    n = df.count()
    df.write.mode('overwrite').parquet(ruta)
    print(f'{tabla:<40} {n:>8} filas  {time.time()-t0:5.1f}s')

spark.stop()
