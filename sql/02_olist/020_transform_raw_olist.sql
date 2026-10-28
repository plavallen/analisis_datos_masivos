-- Transformación raw → olist.
-- Conversión de tipos con NULLIF(x,'') para manejar vacíos como NULL.
-- Normalización de prefijos postales con lpad(x, 5, '0').
-- Si alguna conversión de tipo falla, PostgreSQL lanzará un error
-- (ON_ERROR_STOP=1 garantiza que se detecte, no se descarte en silencio).
-- Ejecutar DESPUÉS de 010_ddl_olist.sql.

-- ── 1. geo_cep (tabla derivada de raw.geolocation) ───────────────────────────
-- Una fila por prefijo postal.
-- lat, lng: mediana (percentile_cont); city, state: moda (mode() WITHIN GROUP).
INSERT INTO olist.geo_cep (zip_prefix, lat, lng, city, state)
SELECT
    lpad(geolocation_zip_code_prefix, 5, '0'),
    percentile_cont(0.5) WITHIN GROUP (ORDER BY geolocation_lat::double precision),
    percentile_cont(0.5) WITHIN GROUP (ORDER BY geolocation_lng::double precision),
    mode() WITHIN GROUP (ORDER BY geolocation_city),
    mode() WITHIN GROUP (ORDER BY geolocation_state)
FROM raw.geolocation
WHERE geolocation_zip_code_prefix IS NOT NULL
  AND geolocation_zip_code_prefix <> ''
GROUP BY geolocation_zip_code_prefix;

-- ── 2. geolocation (copia tipada) ─────────────────────────────────────────────
INSERT INTO olist.geolocation
    (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
     geolocation_city, geolocation_state)
SELECT
    lpad(geolocation_zip_code_prefix, 5, '0'),
    NULLIF(geolocation_lat, '')::double precision,
    NULLIF(geolocation_lng, '')::double precision,
    geolocation_city,
    geolocation_state
FROM raw.geolocation;

-- ── 3. category_translation ───────────────────────────────────────────────────
INSERT INTO olist.category_translation
    (product_category_name, product_category_name_english)
SELECT
    product_category_name,
    product_category_name_english
FROM raw.category_translation
WHERE product_category_name         IS NOT NULL
  AND product_category_name         <> ''
  AND product_category_name_english IS NOT NULL
  AND product_category_name_english <> '';

-- ── 4. customers ──────────────────────────────────────────────────────────────
INSERT INTO olist.customers
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state)
SELECT
    customer_id,
    customer_unique_id,
    lpad(customer_zip_code_prefix, 5, '0'),
    customer_city,
    customer_state
FROM raw.customers;

-- ── 5. sellers ────────────────────────────────────────────────────────────────
INSERT INTO olist.sellers
    (seller_id, seller_zip_code_prefix, seller_city, seller_state)
SELECT
    seller_id,
    lpad(seller_zip_code_prefix, 5, '0'),
    seller_city,
    seller_state
FROM raw.sellers;

-- ── 6. products ───────────────────────────────────────────────────────────────
-- Renombres: product_name_lenght → product_name_length (idem description).
INSERT INTO olist.products
    (product_id, product_category_name,
     product_name_length, product_description_length,
     product_photos_qty, product_weight_g,
     product_length_cm, product_height_cm, product_width_cm)
SELECT
    product_id,
    NULLIF(product_category_name, ''),
    NULLIF(product_name_lenght, '')::integer,
    NULLIF(product_description_lenght, '')::integer,
    NULLIF(product_photos_qty, '')::smallint,
    NULLIF(product_weight_g, '')::integer,
    NULLIF(product_length_cm, '')::integer,
    NULLIF(product_height_cm, '')::integer,
    NULLIF(product_width_cm, '')::integer
FROM raw.products;

-- ── 7. orders ─────────────────────────────────────────────────────────────────
INSERT INTO olist.orders
    (order_id, customer_id, order_status,
     order_purchase_timestamp, order_approved_at,
     order_delivered_carrier_date, order_delivered_customer_date,
     order_estimated_delivery_date)
SELECT
    order_id,
    customer_id,
    order_status,
    NULLIF(order_purchase_timestamp, '')::timestamp,
    NULLIF(order_approved_at, '')::timestamp,
    NULLIF(order_delivered_carrier_date, '')::timestamp,
    NULLIF(order_delivered_customer_date, '')::timestamp,
    NULLIF(order_estimated_delivery_date, '')::timestamp
FROM raw.orders;

-- ── 8. order_items ────────────────────────────────────────────────────────────
INSERT INTO olist.order_items
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value)
SELECT
    order_id,
    order_item_id::smallint,
    product_id,
    seller_id,
    NULLIF(shipping_limit_date, '')::timestamp,
    NULLIF(price, '')::numeric(10,2),
    NULLIF(freight_value, '')::numeric(10,2)
FROM raw.order_items;

-- ── 9. order_payments ─────────────────────────────────────────────────────────
INSERT INTO olist.order_payments
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value)
SELECT
    order_id,
    payment_sequential::smallint,
    NULLIF(payment_type, ''),
    NULLIF(payment_installments, '')::smallint,
    NULLIF(payment_value, '')::numeric(10,2)
FROM raw.order_payments;

-- ── 10. order_reviews ─────────────────────────────────────────────────────────
INSERT INTO olist.order_reviews
    (review_id, order_id, review_score,
     review_comment_title, review_comment_message,
     review_creation_date, review_answer_timestamp)
SELECT
    review_id,
    order_id,
    NULLIF(review_score, '')::smallint,
    NULLIF(review_comment_title, ''),
    NULLIF(review_comment_message, ''),
    NULLIF(review_creation_date, '')::timestamp,
    NULLIF(review_answer_timestamp, '')::timestamp
FROM raw.order_reviews;
