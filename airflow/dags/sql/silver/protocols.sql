-- E3/silver — protocols: aplica os eventos CDC de UM tenant (MERGE).
-- Tabela append-only na origem, mas o MERGE cobre igualmente eventuais
-- updates/deletes (ex.: expurgo) sem tratamento especial.
-- Ver processes.sql para a semântica de placeholders e dedup.

MERGE INTO iceberg.silver.protocols AS t
USING (
    SELECT tenant_id, id, process_id, event_type, detail, created_at, op
    FROM (
        SELECT
            tenant_id,
            CAST(json_extract_scalar(COALESCE(after_json, before_json), '$.id') AS integer) AS id,
            CAST(json_extract_scalar(after_json, '$.process_id') AS integer)                AS process_id,
            json_extract_scalar(after_json, '$.event_type')                                 AS event_type,
            json_extract_scalar(after_json, '$.detail')                                     AS detail,
            from_iso8601_timestamp(json_extract_scalar(after_json, '$.created_at'))         AS created_at,
            op,
            row_number() OVER (
                PARTITION BY tenant_id,
                             json_extract_scalar(COALESCE(after_json, before_json), '$.id')
                ORDER BY COALESCE(lsn, 0) DESC, ts_ms DESC
            ) AS rn
        FROM iceberg.bronze.cdc_events
        WHERE tenant_id = '{tenant_id}'
          AND source_table = 'protocols'
          AND extracted_at >= from_iso8601_timestamp('{since}')
    )
    WHERE rn = 1
) AS s
ON t.tenant_id = s.tenant_id AND t.id = s.id
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    process_id = s.process_id,
    event_type = s.event_type,
    detail     = s.detail,
    created_at = s.created_at
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT
    (tenant_id, id, process_id, event_type, detail, created_at)
    VALUES (s.tenant_id, s.id, s.process_id, s.event_type, s.detail, s.created_at)
