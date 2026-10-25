-- Usuario de solo lectura para Superset.
-- La contraseña se inyecta con envsubst desde el Makefile (make doris).

CREATE USER IF NOT EXISTS '${DORIS_SUPERSET_USER}'@'%' IDENTIFIED BY '${DORIS_SUPERSET_PASS}';

GRANT SELECT_PRIV ON olist_bi.* TO '${DORIS_SUPERSET_USER}'@'%';
GRANT SELECT_PRIV ON lake.gold.* TO '${DORIS_SUPERSET_USER}'@'%';
