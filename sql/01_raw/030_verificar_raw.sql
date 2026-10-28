-- Verificación de conteos raw vs. parser CSV.
-- Fuente de referencia: mediciones/etapa1/entorno/conteo_csv_parser.txt
-- Resultado esperado: cada tabla debe tener la cantidad exacta indicada.

SELECT
    schemaname,
    relname                 AS tabla,
    n_live_tup              AS filas_stats,  -- estimación del planificador
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) AS tamaño
FROM pg_stat_user_tables
WHERE schemaname = 'raw'
ORDER BY relname;

-- Conteo exacto (puede ser lento en geolocation con 1M filas)
SELECT 'customers'           AS tabla, COUNT(*) AS filas FROM raw.customers           UNION ALL
SELECT 'geolocation',                  COUNT(*)          FROM raw.geolocation          UNION ALL
SELECT 'order_items',                  COUNT(*)          FROM raw.order_items          UNION ALL
SELECT 'order_payments',               COUNT(*)          FROM raw.order_payments       UNION ALL
SELECT 'order_reviews',                COUNT(*)          FROM raw.order_reviews        UNION ALL
SELECT 'orders',                       COUNT(*)          FROM raw.orders               UNION ALL
SELECT 'products',                     COUNT(*)          FROM raw.products             UNION ALL
SELECT 'sellers',                      COUNT(*)          FROM raw.sellers              UNION ALL
SELECT 'category_translation',         COUNT(*)          FROM raw.category_translation
ORDER BY tabla;
