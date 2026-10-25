USE olist_bi;

CREATE VIEW IF NOT EXISTS pedidos AS
SELECT * FROM lake.gold.pedidos;

DROP VIEW IF EXISTS envios;
CREATE VIEW envios AS
SELECT *,
    DATE(CONCAT(anio_mes, '-01')) AS fecha_mes
FROM lake.gold.envios;

CREATE VIEW IF NOT EXISTS clientes AS
SELECT * FROM lake.gold.clientes;

CREATE VIEW IF NOT EXISTS vendedores AS
SELECT * FROM lake.gold.vendedores;

CREATE VIEW IF NOT EXISTS ciudades_mapa AS
SELECT * FROM lake.gold.ciudades_mapa;

DROP VIEW IF EXISTS descomposicion_mes;
CREATE VIEW descomposicion_mes AS
SELECT d.*,
    ROUND(s.n_tardios * 1.0 / NULLIF(s.n_entregados, 0), 6) AS tasa_tardios
FROM lake.gold.descomposicion_mes d
LEFT JOIN serie_mensual s ON d.mes = s.anio_mes;

CREATE VIEW IF NOT EXISTS promesa_ruta AS
SELECT * FROM lake.gold.promesa_ruta;

CREATE VIEW IF NOT EXISTS embudo_limites AS
SELECT * FROM lake.gold.embudo_limites;

CREATE VIEW IF NOT EXISTS eventos AS
SELECT * FROM lake.gold.eventos;

CREATE VIEW IF NOT EXISTS calidad_hallazgos AS
SELECT * FROM lake.gold.calidad_hallazgos;

CREATE VIEW IF NOT EXISTS pedidos_entregados_periodo AS
SELECT *
FROM lake.gold.pedidos
WHERE order_status = 'delivered'
  AND en_periodo_util = TRUE;

CREATE VIEW IF NOT EXISTS serie_mensual AS
SELECT
    anio_mes,
    COUNT(order_id)                                                AS n_pedidos,
    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END)   AS n_entregados,
    SUM(CASE WHEN tarde = TRUE THEN 1 ELSE 0 END)                  AS n_tardios,
    SUM(CASE WHEN order_status IN ('canceled','unavailable') THEN 1 ELSE 0 END) AS n_cancelados,
    SUM(gmv)                                                       AS gmv_total,
    COUNT(DISTINCT customer_unique_id)                             AS n_clientes,
    SUM(CASE WHEN review_negativa = TRUE THEN 1 ELSE 0 END)        AS n_resenas_negativas,
    SUM(CASE WHEN review_score IS NOT NULL THEN 1 ELSE 0 END)      AS n_con_resena
FROM lake.gold.pedidos
WHERE en_periodo_util = TRUE
GROUP BY anio_mes;

CREATE VIEW IF NOT EXISTS serie_diaria AS
SELECT
    fecha_compra,
    COUNT(order_id)                                                AS n_pedidos,
    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END)   AS n_entregados,
    SUM(CASE WHEN tarde = TRUE THEN 1 ELSE 0 END)                  AS n_tardios,
    SUM(gmv)                                                       AS gmv_total
FROM lake.gold.pedidos
WHERE en_periodo_util = TRUE
GROUP BY fecha_compra;
