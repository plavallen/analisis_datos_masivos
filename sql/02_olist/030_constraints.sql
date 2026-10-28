-- Claves foráneas del esquema olist.
-- Estrategia: agregar todas con NOT VALID, luego intentar VALIDATE CONSTRAINT.
-- Las FK que fallen la validación quedan como NOT VALID;
-- las violaciones se cuantifican con anti-joins al final del script.
-- Sin índices en columnas FK: son el experimento de la Fase 1.5.

-- ── FK de negocio (se espera que sean válidas) ────────────────────────────────

ALTER TABLE olist.orders
    ADD CONSTRAINT fk_orders_customers
    FOREIGN KEY (customer_id) REFERENCES olist.customers(customer_id)
    NOT VALID;

ALTER TABLE olist.order_items
    ADD CONSTRAINT fk_order_items_orders
    FOREIGN KEY (order_id) REFERENCES olist.orders(order_id)
    NOT VALID;

ALTER TABLE olist.order_items
    ADD CONSTRAINT fk_order_items_products
    FOREIGN KEY (product_id) REFERENCES olist.products(product_id)
    NOT VALID;

ALTER TABLE olist.order_items
    ADD CONSTRAINT fk_order_items_sellers
    FOREIGN KEY (seller_id) REFERENCES olist.sellers(seller_id)
    NOT VALID;

ALTER TABLE olist.order_payments
    ADD CONSTRAINT fk_order_payments_orders
    FOREIGN KEY (order_id) REFERENCES olist.orders(order_id)
    NOT VALID;

ALTER TABLE olist.order_reviews
    ADD CONSTRAINT fk_order_reviews_orders
    FOREIGN KEY (order_id) REFERENCES olist.orders(order_id)
    NOT VALID;

-- ── FK con prefijos postales (esperamos violaciones) ─────────────────────────

ALTER TABLE olist.customers
    ADD CONSTRAINT fk_customers_geo_cep
    FOREIGN KEY (customer_zip_code_prefix) REFERENCES olist.geo_cep(zip_prefix)
    NOT VALID;

ALTER TABLE olist.sellers
    ADD CONSTRAINT fk_sellers_geo_cep
    FOREIGN KEY (seller_zip_code_prefix) REFERENCES olist.geo_cep(zip_prefix)
    NOT VALID;

-- ── FK de categoría (esperamos violaciones por NULL o categorías sin traducción)

ALTER TABLE olist.products
    ADD CONSTRAINT fk_products_category
    FOREIGN KEY (product_category_name)
    REFERENCES olist.category_translation(product_category_name)
    NOT VALID;

-- ── Intentar validar cada FK; registrar resultado ─────────────────────────────

DO $$
DECLARE
    fk_list TEXT[] := ARRAY[
        'olist.orders|fk_orders_customers',
        'olist.order_items|fk_order_items_orders',
        'olist.order_items|fk_order_items_products',
        'olist.order_items|fk_order_items_sellers',
        'olist.order_payments|fk_order_payments_orders',
        'olist.order_reviews|fk_order_reviews_orders',
        'olist.customers|fk_customers_geo_cep',
        'olist.sellers|fk_sellers_geo_cep',
        'olist.products|fk_products_category'
    ];
    item TEXT;
    tbl  TEXT;
    con  TEXT;
BEGIN
    FOREACH item IN ARRAY fk_list LOOP
        tbl := split_part(item, '|', 1);
        con := split_part(item, '|', 2);
        BEGIN
            EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT %I', tbl, con);
            RAISE NOTICE 'VALIDADA: %.%', tbl, con;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'NOT VALID (violaciones): %.% — %', tbl, con, SQLERRM;
        END;
    END LOOP;
END;
$$;

-- ── Cuantificación de violaciones con anti-joins ──────────────────────────────

-- FK customers → geo_cep
SELECT 'customers sin geo_cep' AS fk,
       COUNT(*) AS violaciones
FROM olist.customers c
WHERE NOT EXISTS (
    SELECT 1 FROM olist.geo_cep g WHERE g.zip_prefix = c.customer_zip_code_prefix
);

-- FK sellers → geo_cep
SELECT 'sellers sin geo_cep' AS fk,
       COUNT(*) AS violaciones
FROM olist.sellers s
WHERE NOT EXISTS (
    SELECT 1 FROM olist.geo_cep g WHERE g.zip_prefix = s.seller_zip_code_prefix
);

-- FK products → category_translation (solo los no NULL)
SELECT 'products sin categoría traducida' AS fk,
       COUNT(*) AS violaciones
FROM olist.products p
WHERE p.product_category_name IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM olist.category_translation ct
      WHERE ct.product_category_name = p.product_category_name
  );

-- FK orders → customers (debe ser 0)
SELECT 'orders sin customer' AS fk,
       COUNT(*) AS violaciones
FROM olist.orders o
WHERE NOT EXISTS (
    SELECT 1 FROM olist.customers c WHERE c.customer_id = o.customer_id
);

-- FK order_items → orders (debe ser 0)
SELECT 'order_items sin order' AS fk,
       COUNT(*) AS violaciones
FROM olist.order_items oi
WHERE NOT EXISTS (
    SELECT 1 FROM olist.orders o WHERE o.order_id = oi.order_id
);

-- FK order_reviews → orders (debe ser 0)
SELECT 'order_reviews sin order' AS fk,
       COUNT(*) AS violaciones
FROM olist.order_reviews r
WHERE NOT EXISTS (
    SELECT 1 FROM olist.orders o WHERE o.order_id = r.order_id
);
