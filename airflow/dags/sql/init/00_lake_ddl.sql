-- E2/E3 — DDL do lake (idempotente, roda a cada ciclo).
-- Tudo particionado por tenant_id: segregação física dentro do lake interno.
-- (a bronze é criada pela landing via PyIceberg — dona do próprio contrato)

CREATE SCHEMA IF NOT EXISTS iceberg.bronze;

CREATE SCHEMA IF NOT EXISTS iceberg.silver;

CREATE SCHEMA IF NOT EXISTS iceberg.gold;

CREATE TABLE IF NOT EXISTS iceberg.silver.process_types (
    tenant_id  varchar,
    id         integer,
    name       varchar,
    sla_hours  integer
) WITH (partitioning = ARRAY['tenant_id']);

CREATE TABLE IF NOT EXISTS iceberg.silver.processes (
    tenant_id       varchar,
    id              integer,
    process_type_id integer,
    title           varchar,
    status          varchar,
    opened_at       timestamp(6) with time zone,
    closed_at       timestamp(6) with time zone,
    deleted         boolean
) WITH (partitioning = ARRAY['tenant_id']);

CREATE TABLE IF NOT EXISTS iceberg.silver.protocols (
    tenant_id  varchar,
    id         integer,
    process_id integer,
    event_type varchar,
    detail     varchar,
    created_at timestamp(6) with time zone
) WITH (partitioning = ARRAY['tenant_id']);

CREATE TABLE IF NOT EXISTS iceberg.gold.process_summary_by_type (
    tenant_id          varchar,
    process_type       varchar,
    total_processes    bigint,
    open_processes     bigint,
    closed_processes   bigint,
    canceled_processes bigint,
    avg_duration_hours double,
    computed_at        timestamp(6) with time zone
) WITH (partitioning = ARRAY['tenant_id']);

CREATE TABLE IF NOT EXISTS iceberg.gold.daily_activity (
    tenant_id        varchar,
    activity_date    date,
    processes_opened bigint,
    processes_closed bigint,
    protocols_logged bigint,
    computed_at      timestamp(6) with time zone
) WITH (partitioning = ARRAY['tenant_id'])
