-- E0 — Schema de exemplo de um tenant: BPMS simplificado.
-- Executado uma vez por database de tenant, com variáveis psql:
--   :seed  → semente do random (reprodutível, diferente por tenant)
--   :nproc → quantidade de processos gerados
--
-- Observação: NÃO existe coluna tenant_id — o tenant é o próprio database,
-- como no produto real. Quem injeta o tenant_id no fluxo é a extração (E1),
-- via nome do banco/tópico.

CREATE TABLE process_types (
    id        SERIAL PRIMARY KEY,
    name      TEXT NOT NULL,
    sla_hours INT  NOT NULL
);

CREATE TABLE processes (
    id              SERIAL PRIMARY KEY,
    process_type_id INT  NOT NULL REFERENCES process_types (id),
    title           TEXT NOT NULL,
    status          TEXT NOT NULL, -- open | in_progress | closed | canceled
    opened_at       TIMESTAMPTZ NOT NULL,
    closed_at       TIMESTAMPTZ,
    deleted         BOOLEAN NOT NULL DEFAULT false -- soft delete (dado sensível)
);

CREATE TABLE protocols (
    id         SERIAL PRIMARY KEY,
    process_id INT  NOT NULL REFERENCES processes (id),
    event_type TEXT NOT NULL, -- opened | moved | commented | assigned | closed
    detail     TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

-- ─── Seed sintético reprodutível ────────────────────────────────────────────
SELECT setseed(:seed);

INSERT INTO process_types (name, sla_hours) VALUES
    ('Reembolso', 72),
    ('Contratação', 240),
    ('Suporte interno', 24),
    ('Compra', 120),
    ('Auditoria', 360);

INSERT INTO processes (process_type_id, title, status, opened_at, closed_at, deleted)
SELECT
    1 + floor(random() * 5)::int,
    'Processo ' || i,
    (ARRAY['open', 'in_progress', 'closed', 'closed', 'canceled'])[1 + floor(random() * 5)::int],
    now() - (random() * interval '180 days'),
    NULL,
    random() < 0.02
FROM generate_series(1, :nproc) AS i;

-- encerrados/cancelados ganham data de fechamento coerente com a abertura
UPDATE processes
SET closed_at = opened_at + (random() * interval '30 days')
WHERE status IN ('closed', 'canceled');

-- protocolo de abertura (1 por processo, no instante da abertura)
INSERT INTO protocols (process_id, event_type, detail, created_at)
SELECT id, 'opened', 'abertura do processo', opened_at
FROM processes;

-- movimentações intermediárias (0..3 por processo)
INSERT INTO protocols (process_id, event_type, detail, created_at)
SELECT
    p.id,
    (ARRAY['moved', 'commented', 'assigned'])[1 + floor(random() * 3)::int],
    'movimentação automática',
    p.opened_at + (random() * interval '20 days')
FROM processes p
CROSS JOIN generate_series(1, 3) AS g
WHERE random() < 0.6;

-- protocolo de encerramento para quem fechou
INSERT INTO protocols (process_id, event_type, detail, created_at)
SELECT id, 'closed', 'encerramento do processo', closed_at
FROM processes
WHERE closed_at IS NOT NULL;
