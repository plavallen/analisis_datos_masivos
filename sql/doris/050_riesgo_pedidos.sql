-- Vista de la predicción de riesgo de atraso para Superset (dataset riesgo_pedidos).
-- Va aparte de 030_vistas.sql porque lake.gold.riesgo_pedidos solo existe después
-- del scoring (etapa2/scripts/riesgo_pedidos.py); Doris valida la tabla al crear la vista.
-- Se aplica con: make doris-riesgo
--
-- PROVISIONAL: semana_prometida se define como el lunes de la semana de fecha_prometida
-- (el gráfico "Pedidos en riesgo por semana prometida" la usa como eje con formato
-- "Sem. %d/%m"). Reemplazar por la definición original cuando esté disponible.
SWITCH internal;
USE olist_bi;

DROP VIEW IF EXISTS riesgo_pedidos;

-- La regla Vencido se aplica aquí para que funcione independientemente del CASE
-- que use el script de scoring (minúsculas o mayúsculas, con o sin Vencido en origen).
CREATE VIEW riesgo_pedidos AS
SELECT
    order_id,
    seller_id,
    ruta,
    categoria,
    fecha_prometida,
    dias_hasta_promesa,
    puntaje,
    percentil_puntaje,
    CASE
        WHEN dias_hasta_promesa < 0        THEN 'Vencido'
        WHEN LOWER(nivel_riesgo) = 'alto'  THEN 'Alto'
        WHEN LOWER(nivel_riesgo) = 'medio' THEN 'Medio'
        WHEN LOWER(nivel_riesgo) = 'bajo'  THEN 'Bajo'
        ELSE nivel_riesgo
    END AS nivel_riesgo,
    CASE WHEN dias_hasta_promesa < 0 THEN 4 ELSE riesgo_num END AS riesgo_num,
    riesgo_relativo,
    version_modelo,
    fecha_corte,
    CAST(DATE_TRUNC(fecha_prometida, 'week') AS DATE) AS semana_prometida
FROM lake.gold.riesgo_pedidos;
