-- COMMENT ON TABLE y COMMENT ON COLUMN para todos los objetos de olist.
-- Fuente para la generación del diccionario de datos (docs/diccionario_datos.md).

-- ── geo_cep ───────────────────────────────────────────────────────────────────
COMMENT ON TABLE olist.geo_cep IS
    'Tabla derivada: un registro por prefijo postal con coordenadas medianas '
    '(mediana de lat/lng) y localización más frecuente (moda de ciudad y estado). '
    'Generada a partir de raw.geolocation con percentile_cont y mode().';
COMMENT ON COLUMN olist.geo_cep.zip_prefix  IS 'Prefijo postal de 5 dígitos (char(5), lpad). Clave primaria.';
COMMENT ON COLUMN olist.geo_cep.lat         IS 'Latitud mediana del prefijo postal.';
COMMENT ON COLUMN olist.geo_cep.lng         IS 'Longitud mediana del prefijo postal.';
COMMENT ON COLUMN olist.geo_cep.city        IS 'Ciudad más frecuente para el prefijo postal.';
COMMENT ON COLUMN olist.geo_cep.state       IS 'Estado brasileño (UF de 2 letras) más frecuente para el prefijo.';

-- ── geolocation ───────────────────────────────────────────────────────────────
COMMENT ON TABLE olist.geolocation IS
    'Copia tipada de raw.geolocation. Múltiples filas por prefijo postal; '
    'sin clave natural. Clave artificial geolocation_id (IDENTITY). '
    'Fuente original: olist_geolocation_dataset.csv.';
COMMENT ON COLUMN olist.geolocation.geolocation_id              IS 'Clave artificial generada (GENERATED ALWAYS AS IDENTITY). No existe en el CSV original.';
COMMENT ON COLUMN olist.geolocation.geolocation_zip_code_prefix IS 'Prefijo postal de 5 dígitos (char(5), lpad). Puede tener múltiples filas por prefijo.';
COMMENT ON COLUMN olist.geolocation.geolocation_lat             IS 'Latitud geográfica del punto.';
COMMENT ON COLUMN olist.geolocation.geolocation_lng             IS 'Longitud geográfica del punto.';
COMMENT ON COLUMN olist.geolocation.geolocation_city            IS 'Nombre de la ciudad asociada al punto.';
COMMENT ON COLUMN olist.geolocation.geolocation_state           IS 'Código de estado brasileño (UF, 2 letras).';

-- ── customers ─────────────────────────────────────────────────────────────────
COMMENT ON TABLE olist.customers IS
    'Dirección de entrega de cada pedido. '
    'IMPORTANTE: customer_id no representa a una persona real; Olist genera '
    'un customer_id nuevo por pedido (relación 1:1 con orders). '
    'La identidad del comprador es customer_unique_id. '
    'Fuente original: olist_customers_dataset.csv.';
COMMENT ON COLUMN olist.customers.customer_id              IS 'Identificador de dirección de entrega; 1:1 con orders.order_id. No representa a una persona.';
COMMENT ON COLUMN olist.customers.customer_unique_id       IS 'Identificador del comprador real (persona). Usar para métricas de recompra.';
COMMENT ON COLUMN olist.customers.customer_zip_code_prefix IS 'Prefijo postal de la dirección de entrega (char(5)). FK → geo_cep.zip_prefix (NOT VALID).';
COMMENT ON COLUMN olist.customers.customer_city            IS 'Nombre de la ciudad de entrega (texto libre del CSV).';
COMMENT ON COLUMN olist.customers.customer_state           IS 'Código de estado brasileño (UF, 2 letras).';

-- ── sellers ───────────────────────────────────────────────────────────────────
COMMENT ON TABLE olist.sellers IS
    'Vendedores del marketplace. '
    'Fuente original: olist_sellers_dataset.csv.';
COMMENT ON COLUMN olist.sellers.seller_id                IS 'Identificador único del vendedor (hash hex, char(32)).';
COMMENT ON COLUMN olist.sellers.seller_zip_code_prefix   IS 'Prefijo postal de la ubicación del vendedor (char(5)). FK → geo_cep.zip_prefix (NOT VALID).';
COMMENT ON COLUMN olist.sellers.seller_city              IS 'Nombre de la ciudad del vendedor.';
COMMENT ON COLUMN olist.sellers.seller_state             IS 'Código de estado brasileño (UF, 2 letras).';

-- ── category_translation ──────────────────────────────────────────────────────
COMMENT ON TABLE olist.category_translation IS
    'Traducción de categorías de producto del portugués al inglés. '
    'Fuente original: product_category_name_translation.csv '
    '(nota: tenía BOM U+FEFF en el encabezado, irrelevante para los datos).';
COMMENT ON COLUMN olist.category_translation.product_category_name         IS 'Nombre de categoría en portugués. Clave primaria.';
COMMENT ON COLUMN olist.category_translation.product_category_name_english IS 'Nombre de categoría traducido al inglés.';

-- ── products ──────────────────────────────────────────────────────────────────
COMMENT ON TABLE olist.products IS
    'Catálogo de productos. '
    'Columnas renombradas respecto del CSV (typos de Kaggle): '
    'product_name_lenght → product_name_length, '
    'product_description_lenght → product_description_length. '
    'Fuente original: olist_products_dataset.csv.';
COMMENT ON COLUMN olist.products.product_id                  IS 'Identificador único del producto (hash hex, char(32)).';
COMMENT ON COLUMN olist.products.product_category_name       IS 'Nombre de categoría en portugués. FK → category_translation (NOT VALID; productos sin categoría o sin traducción).';
COMMENT ON COLUMN olist.products.product_name_length         IS 'Cantidad de caracteres del nombre del producto. Renombrado: product_name_lenght (CSV original).';
COMMENT ON COLUMN olist.products.product_description_length  IS 'Cantidad de caracteres de la descripción. Renombrado: product_description_lenght (CSV original).';
COMMENT ON COLUMN olist.products.product_photos_qty          IS 'Cantidad de fotos del producto.';
COMMENT ON COLUMN olist.products.product_weight_g            IS 'Peso del producto en gramos.';
COMMENT ON COLUMN olist.products.product_length_cm           IS 'Largo del producto en centímetros.';
COMMENT ON COLUMN olist.products.product_height_cm           IS 'Alto del producto en centímetros.';
COMMENT ON COLUMN olist.products.product_width_cm            IS 'Ancho del producto en centímetros.';

-- ── orders ────────────────────────────────────────────────────────────────────
COMMENT ON TABLE olist.orders IS
    'Pedidos del marketplace. Tabla central del modelo. '
    'Fuente original: olist_orders_dataset.csv.';
COMMENT ON COLUMN olist.orders.order_id                       IS 'Identificador único del pedido (hash hex, char(32)).';
COMMENT ON COLUMN olist.orders.customer_id                    IS 'Referencia a la dirección de entrega (olist.customers). FK válida.';
COMMENT ON COLUMN olist.orders.order_status                   IS 'Estado del pedido: delivered, shipped, canceled, unavailable, processing, invoiced, created, approved.';
COMMENT ON COLUMN olist.orders.order_purchase_timestamp       IS 'Fecha y hora de la compra.';
COMMENT ON COLUMN olist.orders.order_approved_at              IS 'Fecha y hora de aprobación del pago. Puede ser NULL.';
COMMENT ON COLUMN olist.orders.order_delivered_carrier_date   IS 'Fecha de entrega al transportista (despacho del vendedor). Puede ser NULL.';
COMMENT ON COLUMN olist.orders.order_delivered_customer_date  IS 'Fecha de entrega al cliente. NULL si aún no entregado.';
COMMENT ON COLUMN olist.orders.order_estimated_delivery_date  IS 'Fecha de entrega prometida al momento de la compra.';

-- ── order_items ───────────────────────────────────────────────────────────────
COMMENT ON TABLE olist.order_items IS
    'Ítems de cada pedido. Un pedido puede tener múltiples ítems (de distintos vendedores). '
    'PK compuesta (order_id, order_item_id). '
    'Fuente original: olist_order_items_dataset.csv.';
COMMENT ON COLUMN olist.order_items.order_id            IS 'Referencia al pedido. FK → orders (válida).';
COMMENT ON COLUMN olist.order_items.order_item_id       IS 'Número secuencial del ítem dentro del pedido (1, 2, …). Max observado: 21.';
COMMENT ON COLUMN olist.order_items.product_id          IS 'Referencia al producto. FK → products (válida).';
COMMENT ON COLUMN olist.order_items.seller_id           IS 'Referencia al vendedor. FK → sellers (válida).';
COMMENT ON COLUMN olist.order_items.shipping_limit_date IS 'Fecha límite para que el vendedor despache el ítem.';
COMMENT ON COLUMN olist.order_items.price               IS 'Precio del ítem (sin flete). En BRL.';
COMMENT ON COLUMN olist.order_items.freight_value       IS 'Costo de flete del ítem. En BRL.';

-- ── order_payments ────────────────────────────────────────────────────────────
COMMENT ON TABLE olist.order_payments IS
    'Pagos de cada pedido. Un pedido puede tener múltiples formas de pago. '
    'PK compuesta (order_id, payment_sequential). '
    'Fuente original: olist_order_payments_dataset.csv.';
COMMENT ON COLUMN olist.order_payments.order_id              IS 'Referencia al pedido. FK → orders (válida).';
COMMENT ON COLUMN olist.order_payments.payment_sequential    IS 'Número secuencial del pago dentro del pedido (1, 2, …). Max observado: 29.';
COMMENT ON COLUMN olist.order_payments.payment_type          IS 'Medio de pago: credit_card, boleto, voucher, debit_card, not_defined.';
COMMENT ON COLUMN olist.order_payments.payment_installments  IS 'Cantidad de cuotas del pago.';
COMMENT ON COLUMN olist.order_payments.payment_value         IS 'Monto del pago en BRL.';

-- ── order_reviews ─────────────────────────────────────────────────────────────
COMMENT ON TABLE olist.order_reviews IS
    'Reseñas de pedidos. review_id no es único solo (98.410 IDs para 99.224 filas); '
    'el par (review_id, order_id) sí es único → PK compuesta. '
    'El campo review_comment_message puede contener saltos de línea. '
    'Fuente original: olist_order_reviews_dataset.csv.';
COMMENT ON COLUMN olist.order_reviews.review_id               IS 'Identificador de la reseña. No único por sí solo; usar con order_id.';
COMMENT ON COLUMN olist.order_reviews.order_id                IS 'Referencia al pedido reseñado. FK → orders (válida).';
COMMENT ON COLUMN olist.order_reviews.review_score            IS 'Puntaje de satisfacción del cliente (1 = peor, 5 = mejor).';
COMMENT ON COLUMN olist.order_reviews.review_comment_title    IS 'Título del comentario (puede ser NULL).';
COMMENT ON COLUMN olist.order_reviews.review_comment_message  IS 'Texto de la reseña en portugués (puede contener saltos de línea; puede ser NULL).';
COMMENT ON COLUMN olist.order_reviews.review_creation_date    IS 'Fecha en que se envió la encuesta al cliente.';
COMMENT ON COLUMN olist.order_reviews.review_answer_timestamp IS 'Fecha en que el cliente respondió la encuesta.';
