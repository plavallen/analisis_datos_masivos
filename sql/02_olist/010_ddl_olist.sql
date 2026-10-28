-- DDL del esquema olist: copia fiel tipada de raw.
-- Tipos correctos, PK, sin FK (se agregan en 030_constraints.sql).
-- Renombres documentados respecto del CSV original.
-- No se limpian datos: sin borrar duplicados ni imputar nulos.
-- Idempotente: DROP ... IF EXISTS CASCADE.

-- ── geo_cep (tabla derivada de geolocation) ──────────────────────────────────
-- Una fila por prefijo postal. Lat/lng = mediana; ciudad/estado = más frecuente.
-- customers y sellers referencian esta tabla, no geolocation directamente.
DROP TABLE IF EXISTS olist.geo_cep CASCADE;
CREATE TABLE olist.geo_cep (
    zip_prefix  char(5)           NOT NULL,
    lat         double precision,
    lng         double precision,
    city        text,
    state       char(2),
    CONSTRAINT pk_geo_cep PRIMARY KEY (zip_prefix)
);

-- ── geolocation (copia tipada con clave artificial) ───────────────────────────
-- Múltiples filas por zip_prefix; la tabla geo_cep las agrega.
DROP TABLE IF EXISTS olist.geolocation CASCADE;
CREATE TABLE olist.geolocation (
    geolocation_id              bigint GENERATED ALWAYS AS IDENTITY,
    geolocation_zip_code_prefix char(5)           NOT NULL,
    geolocation_lat             double precision,
    geolocation_lng             double precision,
    geolocation_city            text,
    geolocation_state           char(2),
    CONSTRAINT pk_geolocation PRIMARY KEY (geolocation_id)
);

-- ── customers ─────────────────────────────────────────────────────────────────
-- customer_id no representa a una persona: Olist genera uno nuevo por pedido.
-- La relación con orders es 1:1. La persona real es customer_unique_id.
DROP TABLE IF EXISTS olist.customers CASCADE;
CREATE TABLE olist.customers (
    customer_id              char(32)  NOT NULL,
    customer_unique_id       char(32)  NOT NULL,
    customer_zip_code_prefix char(5)   NOT NULL,
    customer_city            text,
    customer_state           char(2),
    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

-- ── sellers ───────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS olist.sellers CASCADE;
CREATE TABLE olist.sellers (
    seller_id                char(32)  NOT NULL,
    seller_zip_code_prefix   char(5)   NOT NULL,
    seller_city              text,
    seller_state             char(2),
    CONSTRAINT pk_sellers PRIMARY KEY (seller_id)
);

-- ── category_translation ──────────────────────────────────────────────────────
DROP TABLE IF EXISTS olist.category_translation CASCADE;
CREATE TABLE olist.category_translation (
    product_category_name         text  NOT NULL,
    product_category_name_english text  NOT NULL,
    CONSTRAINT pk_category_translation PRIMARY KEY (product_category_name)
);

-- ── products ──────────────────────────────────────────────────────────────────
-- Renombres respecto del CSV original (typos de Kaggle):
--   product_name_lenght        → product_name_length
--   product_description_lenght → product_description_length
DROP TABLE IF EXISTS olist.products CASCADE;
CREATE TABLE olist.products (
    product_id               char(32)   NOT NULL,
    product_category_name    text,
    product_name_length      integer,   -- renombrado: lenght → length
    product_description_length integer, -- renombrado: lenght → length
    product_photos_qty       smallint,
    product_weight_g         integer,
    product_length_cm        integer,
    product_height_cm        integer,
    product_width_cm         integer,
    CONSTRAINT pk_products PRIMARY KEY (product_id)
);

-- ── orders ────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS olist.orders CASCADE;
CREATE TABLE olist.orders (
    order_id                        char(32)   NOT NULL,
    customer_id                     char(32)   NOT NULL,
    order_status                    text       NOT NULL,
    order_purchase_timestamp        timestamp,
    order_approved_at               timestamp,
    order_delivered_carrier_date    timestamp,
    order_delivered_customer_date   timestamp,
    order_estimated_delivery_date   timestamp,
    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);

-- ── order_items ───────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS olist.order_items CASCADE;
CREATE TABLE olist.order_items (
    order_id            char(32)       NOT NULL,
    order_item_id       smallint       NOT NULL,  -- max observado: 21
    product_id          char(32)       NOT NULL,
    seller_id           char(32)       NOT NULL,
    shipping_limit_date timestamp,
    price               numeric(10,2),
    freight_value       numeric(10,2),
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, order_item_id)
);

-- ── order_payments ────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS olist.order_payments CASCADE;
CREATE TABLE olist.order_payments (
    order_id             char(32)   NOT NULL,
    payment_sequential   smallint   NOT NULL,  -- max observado: 29
    payment_type         text,
    payment_installments smallint,
    payment_value        numeric(10,2),
    CONSTRAINT pk_order_payments PRIMARY KEY (order_id, payment_sequential)
);

-- ── order_reviews ─────────────────────────────────────────────────────────────
-- review_id NO es único: 98.410 únicos para 99.224 filas.
-- El par (review_id, order_id) SÍ es único → PK compuesta.
DROP TABLE IF EXISTS olist.order_reviews CASCADE;
CREATE TABLE olist.order_reviews (
    review_id               char(32)   NOT NULL,
    order_id                char(32)   NOT NULL,
    review_score            smallint,
    review_comment_title    text,
    review_comment_message  text,
    review_creation_date    timestamp,
    review_answer_timestamp timestamp,
    CONSTRAINT pk_order_reviews PRIMARY KEY (review_id, order_id)
);
