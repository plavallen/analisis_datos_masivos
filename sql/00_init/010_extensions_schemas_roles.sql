-- Extensiones, esquemas y rol de solo lectura.
-- Idempotente: seguro de re-ejecutar.
-- Ejecutar como superusuario (POSTGRES_USER).

-- Extensión para métricas de ejecución por consulta.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Esquemas del proyecto (ver sección 4 de CLAUDE.md).
CREATE SCHEMA IF NOT EXISTS raw;        -- copia literal de CSVs
CREATE SCHEMA IF NOT EXISTS olist;      -- copia tipada, sin limpieza
CREATE SCHEMA IF NOT EXISTS escala;     -- réplica sintética ×N (solo benchmarks)
CREATE SCHEMA IF NOT EXISTS analitica;  -- dataset analítico (Etapa 2+)
CREATE SCHEMA IF NOT EXISTS bench;      -- tablas auxiliares para pgbench

-- Rol de solo lectura para Superset (Etapa 4).
-- En Etapa 4 recibirá acceso únicamente a analitica.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'olist_lector') THEN
        CREATE ROLE olist_lector LOGIN PASSWORD 'lector_ro';
    END IF;
END
$$;

DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO olist_lector', current_database());
END
$$;

GRANT USAGE ON SCHEMA analitica TO olist_lector;
-- En Etapa 2, después de crear las vistas: GRANT SELECT ON ALL TABLES IN SCHEMA analitica TO olist_lector;
