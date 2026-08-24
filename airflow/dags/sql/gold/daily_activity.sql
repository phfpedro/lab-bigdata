-- E3/gold — atividade diária por tenant: aberturas, encerramentos e volume
-- de protocolos por dia. Full rebuild idempotente (ver process_summary).

DELETE FROM iceberg.gold.daily_activity;

INSERT INTO iceberg.gold.daily_activity
WITH opened AS (
    SELECT tenant_id, CAST(opened_at AS date) AS d, count(*) AS c
    FROM iceberg.silver.processes
    WHERE NOT COALESCE(deleted, false)
    GROUP BY 1, 2
),
closed AS (
    SELECT tenant_id, CAST(closed_at AS date) AS d, count(*) AS c
    FROM iceberg.silver.processes
    WHERE closed_at IS NOT NULL
      AND NOT COALESCE(deleted, false)
    GROUP BY 1, 2
),
logged AS (
    SELECT tenant_id, CAST(created_at AS date) AS d, count(*) AS c
    FROM iceberg.silver.protocols
    GROUP BY 1, 2
)
SELECT
    COALESCE(o.tenant_id, c.tenant_id, l.tenant_id) AS tenant_id,
    COALESCE(o.d, c.d, l.d)                         AS activity_date,
    COALESCE(o.c, 0)                                AS processes_opened,
    COALESCE(c.c, 0)                                AS processes_closed,
    COALESCE(l.c, 0)                                AS protocols_logged,
    current_timestamp(6)                            AS computed_at
FROM opened AS o
FULL OUTER JOIN closed AS c
  ON c.tenant_id = o.tenant_id AND c.d = o.d
FULL OUTER JOIN logged AS l
  ON l.tenant_id = COALESCE(o.tenant_id, c.tenant_id)
 AND l.d = COALESCE(o.d, c.d)
