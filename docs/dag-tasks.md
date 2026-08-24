# DAG `bpms_analytics` — o que cada task faz

Documentação task a task da DAG principal ([airflow/dags/bpms_analytics.py](../airflow/dags/bpms_analytics.py)). Para arquitetura e contratos entre etapas, ver [arquitetura.md](arquitetura.md).

```text
lake_init → land_events → silver_tenant (×N) → build_gold → publish_tenant (×N) → isolation_check
                              ▲ uma por tenant                    ▲ uma por tenant
tenant_ids ──(alimenta as duas tasks mapeadas)──┘
```

---

## 1. `lake_init`

Cria os schemas/tabelas do lake (`CREATE IF NOT EXISTS`). Roda todo ciclo sem risco — não faz nada se já existir.

## 2. `land_events`

Consome os tópicos Kafka (todos os tenants misturados) e grava cru na `bronze.cdc_events`. Não interpreta o conteúdo — só extrai `tenant_id` (do nome do tópico), `op`, `lsn` e o payload em JSON. Só confirma o offset no Kafka **depois** de gravar no lake (at-least-once; duplicatas são resolvidas na silver).

Código: [stages/landing.py](../airflow/dags/stages/landing.py)

## 3. `tenant_ids`

Lê o [config/tenants.yml](../config/tenants.yml) e devolve a lista de tenants. É essa lista que multiplica as duas tasks seguintes — uma instância por tenant, sem precisar mexer em código quando um tenant novo entra.

Código: [stages/config.py](../airflow/dags/stages/config.py)

## 4. `silver_tenant` (×1 por tenant)

Aplica os eventos do tenant na tabela "estado atual", via `MERGE`: insere o que é novo, atualiza o que mudou, remove o que foi deletado; se o mesmo registro chegou várias vezes no lote, só o mais recente (por `lsn`) vale. Uma task por tenant = falha de um não trava os outros.

Código: [stages/transform.py](../airflow/dags/stages/transform.py) · SQL: [sql/silver/](../airflow/dags/sql/silver/)

## 5. `build_gold`

Recalcula os marts (métricas prontas pro dashboard) de todos os tenants numa passada só — `DELETE` + `INSERT`, particionado por `tenant_id`.

Código: [stages/transform.py](../airflow/dags/stages/transform.py) · SQL: [sql/gold/](../airflow/dags/sql/gold/)

## 6. `publish_tenant` (×1 por tenant)

Copia os marts do tenant — só a partição dele, sem a coluna `tenant_id` — pro Postgres exclusivo dele, e dá `GRANT SELECT` pra role daquele tenant.

Código: [stages/serving.py](../airflow/dags/stages/serving.py)

## 7. `isolation_check`

Testa, a cada ciclo: cada tenant consegue ler o próprio serving (positivo), e nenhum tenant consegue conectar no serving de outro (negativo). Qualquer brecha derruba o pipeline.

Código: [stages/serving.py](../airflow/dags/stages/serving.py)

---

## Resumo

| Task | Em 1 frase |
| --- | --- |
| `lake_init` | Garante que schemas/tabelas do lake existem |
| `land_events` | Esvazia o Kafka e grava cru na bronze |
| `tenant_ids` | Lê o registro de tenants, alimenta as tasks mapeadas |
| `silver_tenant` ×N | MERGE dos eventos do tenant na foto atual |
| `build_gold` | Recalcula as métricas de todos os tenants |
| `publish_tenant` ×N | Copia as métricas do tenant pro serving exclusivo dele |
| `isolation_check` | Prova que nenhuma credencial cruza tenants — senão derruba tudo |
