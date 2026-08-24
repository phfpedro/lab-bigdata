-- E3/silver — process_types: aplica os eventos CDC de UM tenant (MERGE).
-- Ver processes.sql para a semântica de placeholders e dedup.

MERGE INTO iceberg.silver.process_types AS t
USING (
    SELECT tenant_id, id, name, sla_hours, op
    FROM (
        SELECT
            tenant_id,
            CAST(json_extract_scalar(COALESCE(after_json, before_json), '$.id') AS integer) AS id,
            json_extract_scalar(after_json, '$.name')                                       AS name,
            CAST(json_extract_scalar(after_json, '$.sla_hours') AS integer)                 AS sla_hours,
            op,
            row_number() OVER (
                PARTITION BY tenant_id,
                             json_extract_scalar(COALESCE(after_json, before_json), '$.id')
                ORDER BY COALESCE(lsn, 0) DESC, ts_ms DESC
            ) AS rn
        FROM iceberg.bronze.cdc_events
        WHERE tenant_id = '{tenant_id}'
          AND source_table = 'process_types'
          AND extracted_at >= from_iso8601_timestamp('{since}')
    )
    WHERE rn = 1
) AS s
ON t.tenant_id = s.tenant_id AND t.id = s.id
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    name      = s.name,
    sla_hours = s.sla_hours
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT
    (tenant_id, id, name, sla_hours)
    VALUES (s.tenant_id, s.id, s.name, s.sla_hours)
