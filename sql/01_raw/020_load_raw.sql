-- Carga de los CSV en el esquema raw.
-- Usa COPY server-side (no \copy) porque psql corre dentro del contenedor postgres
-- donde /data/raw/ está montado. Funciona tanto con psql -f como con psql -c.
-- Ejecutar DESPUÉS de 010_ddl_raw.sql.

COPY raw.customers
    FROM '/data/raw/olist_customers_dataset.csv'
    WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY raw.geolocation
    FROM '/data/raw/olist_geolocation_dataset.csv'
    WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY raw.orders
    FROM '/data/raw/olist_orders_dataset.csv'
    WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY raw.order_items
    FROM '/data/raw/olist_order_items_dataset.csv'
    WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY raw.order_payments
    FROM '/data/raw/olist_order_payments_dataset.csv'
    WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY raw.order_reviews
    FROM '/data/raw/olist_order_reviews_dataset.csv'
    WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
-- Nota: review_comment_message contiene saltos de línea dentro del campo.
-- El parser CSV de Python (no wc -l) es el método correcto para contar filas.

COPY raw.products
    FROM '/data/raw/olist_products_dataset.csv'
    WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
-- Nota: columnas product_name_lenght y product_description_lenght son typos del CSV original.
-- Se corrigen en olist (Fase 1.2), aquí se mantienen literalmente.

COPY raw.sellers
    FROM '/data/raw/olist_sellers_dataset.csv'
    WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY raw.category_translation
    FROM '/data/raw/product_category_name_translation.csv'
    WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
-- Nota: el CSV original tiene BOM (U+FEFF) en el nombre del primer campo del header.
-- Con HEADER true, el encabezado se descarta; el BOM no contamina los datos.
