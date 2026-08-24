-- E3/silver — processes: aplica os eventos CDC de UM tenant (MERGE idempotente).
-- Placeholders (interpolados pela E3 com valores do registro validado):
--   {tenant_id} → tenant desta execução
--   {since}     → início da janela de eventos (ISO-8601)
--
-- Dedup: último evento por PK vence (lsn do WAL desc, ts_ms desc) — reler a
-- mesma janela não muda o resultado. Debezium serializa timestamptz como
-- string ISO-8601, daí o from_iso8601_timestamp nos campos de data.

MERGE INTO iceberg.silver.processes AS t
USING (
    SELECT tenant_id, id, process_type_id, title, status, opened_at, closed_at, deleted, op
    FROM (
        SELECT
            tenant_id,
            CAST(json_extract_scalar(COALESCE(after_json, before_json), '$.id') AS integer) AS id,
            CAST(json_extract_scalar(after_json, '$.process_type_id') AS integer)           AS process_type_id,
            json_extract_scalar(after_json, '$.title')                                      AS title,
            json_extract_scalar(after_json, '$.status')                                     AS status,
            from_iso8601_timestamp(json_extract_scalar(after_json, '$.opened_at'))          AS opened_at,
            from_iso8601_timestamp(json_extract_scalar(after_json, '$.closed_at'))          AS closed_at,
            CAST(json_extract_scalar(after_json, '$.deleted') AS boolean)                   AS deleted,
            op,
            row_number() OVER (
                PARTITION BY tenant_id,
                             json_extract_scalar(COALESCE(after_json, before_json), '$.id')
                ORDER BY COALESCE(lsn, 0) DESC, ts_ms DESC
            ) AS rn
        FROM iceberg.bronze.cdc_events
        WHERE tenant_id = '{tenant_id}'
          AND source_table = 'processes'
          AND extracted_at >= from_iso8601_timestamp('{since}')
    )
    WHERE rn = 1
) AS s
ON t.tenant_id = s.tenant_id AND t.id = s.id
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    process_type_id = s.process_type_id,
    title           = s.title,
    status          = s.status,
    opened_at       = s.opened_at,
    closed_at       = s.closed_at,
    deleted         = s.deleted
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT
    (tenant_id, id, process_type_id, title, status, opened_at, closed_at, deleted)
    VALUES (s.tenant_id, s.id, s.process_type_id, s.title, s.status, s.opened_at, s.closed_at, s.deleted)
