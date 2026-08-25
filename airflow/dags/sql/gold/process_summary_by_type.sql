-- E3/gold — resumo por tipo de processo (o insight "duração média por tipo").
-- Full rebuild a partir da silver: DELETE+INSERT idempotente, todos os
-- tenants numa passada (o particionamento por tenant_id segrega fisicamente).
-- Nota: DELETE e INSERT são statements separados (autocommit) — janela breve
-- de tabela vazia é aceitável neste projeto (em produção, avaliar staging/branch).

DELETE FROM iceberg.gold.process_summary_by_type;

INSERT INTO iceberg.gold.process_summary_by_type
SELECT
    p.tenant_id,
    pt.name AS process_type,
    count(*) AS total_processes,
    count_if(p.status IN ('open', 'in_progress')) AS open_processes,
    count_if(p.status = 'closed') AS closed_processes,
    count_if(p.status = 'canceled') AS canceled_processes,
    avg(CASE WHEN p.status = 'closed'
             THEN date_diff('hour', p.opened_at, p.closed_at) END) AS avg_duration_hours,
    current_timestamp(6) AS computed_at
FROM iceberg.silver.processes AS p
JOIN iceberg.silver.process_types AS pt
  ON pt.tenant_id = p.tenant_id
 AND pt.id = p.process_type_id
WHERE NOT COALESCE(p.deleted, false)
GROUP BY p.tenant_id, pt.name
