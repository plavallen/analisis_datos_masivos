-- DDL del esquema raw: copia literal de los CSV.
-- Todas las columnas son text, sin restricciones ni índices.
-- Nombres de columnas: idénticos al CSV original (incluye typos de Kaggle).
-- Idempotente: DROP ... IF EXISTS + CREATE.

-- ── customers ────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw.customers CASCADE;
CREATE TABLE raw.customers (
    customer_id              text,
    customer_unique_id       text,
    customer_zip_code_prefix text,
    customer_city            text,
    customer_state           text
);

-- ── geolocation ──────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw.geolocation CASCADE;
CREATE TABLE raw.geolocation (
    geolocation_zip_code_prefix text,
    geolocation_lat             text,
    geolocation_lng             text,
    geolocation_city            text,
    geolocation_state           text
);

-- ── orders ───────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw.orders CASCADE;
CREATE TABLE raw.orders (
    order_id                        text,
    customer_id                     text,
    order_status                    text,
    order_purchase_timestamp        text,
    order_approved_at               text,
    order_delivered_carrier_date    text,
    order_delivered_customer_date   text,
    order_estimated_delivery_date   text
);

-- ── order_items ───────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw.order_items CASCADE;
CREATE TABLE raw.order_items (
    order_id           text,
    order_item_id      text,
    product_id         text,
    seller_id          text,
    shipping_limit_date text,
    price              text,
    freight_value      text
);

-- ── order_payments ────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw.order_payments CASCADE;
CREATE TABLE raw.order_payments (
    order_id              text,
    payment_sequential    text,
    payment_type          text,
    payment_installments  text,
    payment_value         text
);

-- ── order_reviews ─────────────────────────────────────────────────────────────
-- Nota: review_comment_message puede contener saltos de línea dentro del campo.
-- Por eso se usa el parser CSV de Python para contar filas (no wc -l).
DROP TABLE IF EXISTS raw.order_reviews CASCADE;
CREATE TABLE raw.order_reviews (
    review_id               text,
    order_id                text,
    review_score            text,
    review_comment_title    text,
    review_comment_message  text,
    review_creation_date    text,
    review_answer_timestamp text
);

-- ── products ──────────────────────────────────────────────────────────────────
-- Typos originales de Kaggle mantenidos intencionalmente en raw.
-- Se corrigen en olist: product_name_lenght → product_name_length,
--                       product_description_lenght → product_description_length.
DROP TABLE IF EXISTS raw.products CASCADE;
CREATE TABLE raw.products (
    product_id                  text,
    product_category_name       text,
    product_name_lenght         text,   -- typo original del CSV
    product_description_lenght  text,   -- typo original del CSV
    product_photos_qty          text,
    product_weight_g            text,
    product_length_cm           text,
    product_height_cm           text,
    product_width_cm            text
);

-- ── sellers ───────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS raw.sellers CASCADE;
CREATE TABLE raw.sellers (
    seller_id                text,
    seller_zip_code_prefix   text,
    seller_city              text,
    seller_state             text
);

-- ── category_translation ──────────────────────────────────────────────────────
-- El CSV original tiene BOM (U+FEFF) en el nombre del primer campo del header.
-- No afecta la carga con HEADER true. El nombre de columna en raw es el correcto.
DROP TABLE IF EXISTS raw.category_translation CASCADE;
CREATE TABLE raw.category_translation (
    product_category_name         text,
    product_category_name_english text
);
