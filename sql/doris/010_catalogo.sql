-- Catálogo Iceberg sobre RustFS.
-- Las credenciales se inyectan con envsubst desde el Makefile (make doris).
-- RUSTFS_ACCESS_KEY y RUSTFS_SECRET_KEY deben estar en .env.

CREATE CATALOG IF NOT EXISTS lake PROPERTIES (
    "type"                          = "iceberg",
    "iceberg.catalog.type"          = "hadoop",
    "warehouse"                     = "s3a://olist/warehouse",
    "s3.endpoint"                   = "http://rustfs:9000",
    "s3.access_key"                 = "${RUSTFS_ACCESS_KEY}",
    "s3.secret_key"                 = "${RUSTFS_SECRET_KEY}",
    "s3.region"                     = "us-east-1",
    "use_path_style"                = "true"
);

SWITCH lake;
SHOW DATABASES;
