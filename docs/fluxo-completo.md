# Fluxo completo, em ordem cronológica

Este documento é o "assista o filme": o que acontece, **na ordem em que
acontece**, do `docker compose up` até o gráfico aparecer no Metabase — e por
que cada passo existe. Ele não substitui os outros documentos, é a porta de
entrada para eles:

- A visão por **camadas/contratos** (a foto parada, não a linha do tempo) está em [arquitetura.md](arquitetura.md).
- Task a task da DAG está em [dag-tasks.md](dag-tasks.md).
- Termos em linguagem simples estão em [glossario.md](glossario.md).
- Referência rápida (1 linha por tecnologia) está em [stack.md](stack.md).
- **Aprofundamento por sistema** (o que é, por que foi escolhido, como
  funciona por dentro, o que estudar depois) está em [sistemas/](sistemas/) —
  um arquivo por tecnologia, linkado a cada passo abaixo.

> Regra de leitura: cada passo tem um "📖 aprofundar em" apontando pro
> documento do sistema envolvido. Leia este arquivo primeiro, do início ao
> fim, para pegar o fluxo geral — depois volte nos links pra entender cada
> peça em detalhe.

---

## Fase 0 — Subir o stack (`docker compose up -d --build`)

Antes de qualquer dado se mover, o Docker Compose sobe ~13 serviços na ordem
que o `depends_on` de cada um exige (ver [docker-compose.yml](../docker-compose.yml)).
Em ordem aproximada de "ficar pronto":

1. **`tenant-db`** (Postgres) sobe com `wal_level=logical` — pré-requisito
   para captura de mudanças (CDC) mais adiante. Ao iniciar pela 1ª vez, roda
   [platform/tenant-db/init-tenants.sh](../platform/tenant-db/init-tenants.sh): lê
   [config/tenants.yml](../config/tenants.yml) e, para cada tenant, cria **um
   database** com o nome do tenant e popula com dados sintéticos de exemplo
   (processos de um BPMS fictício) via
   [tenant_schema.sql](../platform/tenant-db/tenant_schema.sql).
   📖 aprofundar em [sistemas/postgres.md](sistemas/postgres.md)

2. **`serving-db`** (outro Postgres, outra instância) sobe em paralelo e roda
   [platform/serving-db/init-serving.sh](../platform/serving-db/init-serving.sh):
   para cada tenant, cria um database `serving_<tenant>` **vazio** e uma role
   `svc_<tenant>` com senha própria, e já revoga `CONNECT` do `PUBLIC` —
   a parede de isolamento nasce aqui, antes mesmo de existir qualquer dado.
   📖 aprofundar em [sistemas/postgres.md](sistemas/postgres.md)

3. **`postgres-airflow`** sobe — é só o banco de metadados do Airflow (DAG
   runs, logs, conexões), não tem dado de negócio.

4. **`kafka`** sobe em modo standalone (KRaft, sem Zookeeper, 1 nó).
   📖 aprofundar em [sistemas/kafka.md](sistemas/kafka.md)

5. **`connect`** (Kafka Connect, com o plugin Debezium dentro) sobe depois do
   Kafka e do `tenant-db` estarem saudáveis.
   📖 aprofundar em [sistemas/debezium.md](sistemas/debezium.md)

6. **`connect-init`** roda **uma vez** e morre: executa
   [platform/connect/register-connectors.sh](../platform/connect/register-connectors.sh),
   que lê `config/tenants.yml` e `config/tables.yml` e registra, via API REST
   do Kafka Connect, **um conector Debezium por tenant** apontando pro
   database daquele tenant no `tenant-db`. É neste exato momento que o
   `tenant_id` **nasce** no fluxo — como nome de tópico Kafka.

7. **`minio`** sobe, e **`minio-init`** roda uma vez para criar o bucket
   `lake` — o "S3 falso" do laboratório.
   📖 aprofundar em [sistemas/minio.md](sistemas/minio.md)

8. **`iceberg-catalog`** sobe apontando pro bucket do MinIO — é o catálogo
   que sabe "quais tabelas Iceberg existem e onde estão os arquivos".
   📖 aprofundar em [sistemas/iceberg.md](sistemas/iceberg.md)

9. **`trino`** sobe por último entre os motores, lendo a configuração do
   catálogo Iceberg em `platform/trino/catalog/`.
   📖 aprofundar em [sistemas/trino.md](sistemas/trino.md)

10. **`airflow`** sobe por último de tudo (depende de todos os outros
    estarem saudáveis) e carrega a DAG `bpms_analytics` do disco. Ele **não
    dispara nada sozinho** neste momento — só fica pronto e à espera do
    próximo horário do cron (a cada 5 min) ou de um trigger manual.
    📖 aprofundar em [sistemas/airflow.md](sistemas/airflow.md)

11. **`metabase`** sobe por último — fica esperando ser configurado
    manualmente na 1ª vez (ver [README.md](../README.md)).
    📖 aprofundar em [sistemas/metabase.md](sistemas/metabase.md)

**Resultado da Fase 0**: origem com dados sintéticos, conectores CDC
capturando desde já, lake vazio (só o bucket existe), Airflow de prontidão.
Nenhum dado ainda chegou ao lake — os eventos capturados pelo Debezium desde
o registro do conector já estão **acumulados no Kafka**, esperando alguém ler.

---

## Fase 1 — Disparo do pipeline (trigger da DAG, manual ou a cada 5 min)

A partir daqui, tudo roda dentro da DAG `bpms_analytics`
([airflow/dags/bpms_analytics.py](../airflow/dags/bpms_analytics.py)), na ordem
definida por ela. 📖 aprofundar em [sistemas/airflow.md](sistemas/airflow.md)
e task a task em [dag-tasks.md](dag-tasks.md).

### 1. `lake_init`

Roda o DDL idempotente ([sql/init/00_lake_ddl.sql](../airflow/dags/sql/init/00_lake_ddl.sql))
via Trino: garante que os schemas `bronze`/`silver`/`gold` e as tabelas
Iceberg existem. Não faz nada se já existirem — seguro rodar todo ciclo.

### 2. `land_events`

A task [stages/landing.py](../airflow/dags/stages/landing.py) conecta como
**consumer** no Kafka, lê (drena) todos os tópicos `<tenant>.public.<tabela>`
de todos os tenants misturados, e grava cru — sem interpretar o conteúdo —
na tabela Iceberg `bronze.cdc_events`, no MinIO. Só confirma o offset de
leitura no Kafka **depois** de garantir que os dados já foram gravados no
lake (at-least-once: preferir reler um evento a perder um).

Por que "cru"? Porque a bronze não sabe (nem precisa saber) o schema de
negócio de cada tabela — ela só empacota `before`/`after`/`op` em JSON. Isso
significa que uma tabela nova na origem não quebra essa etapa.

📖 aprofundar em [sistemas/kafka.md](sistemas/kafka.md) (o que é consumir/
commitar offset) e [sistemas/iceberg.md](sistemas/iceberg.md) (o que é um
`append` numa tabela Iceberg).

### 3. `tenant_ids`

Task simples ([stages/config.py](../airflow/dags/stages/config.py)) que lê
`config/tenants.yml` e devolve a lista de tenants ativos. Essa lista é o que
faz as duas próximas tasks se **multiplicarem automaticamente** (dynamic task
mapping) — uma cópia por tenant, sem precisar tocar em código quando um
tenant novo é cadastrado.

### 4. `silver_tenant` — uma instância por tenant, em paralelo (até 2 por vez)

Para cada tenant, [stages/transform.py](../airflow/dags/stages/transform.py)
roda, via Trino, um `MERGE` por tabela de negócio
(ver [sql/silver/](../airflow/dags/sql/silver/)): lê da bronze só os eventos
daquele tenant dentro da janela do ciclo (com 24h de lookback de segurança),
deduplica (se o mesmo registro mudou 3x no lote, só a versão mais recente por
`lsn`/`ts_ms` conta) e aplica na tabela silver — insere o que é novo, atualiza
o que mudou, remove o que foi deletado. O resultado é uma "foto do estado
atual" por tabela, particionada por tenant.

Uma task por tenant significa que um evento problemático de um tenant não
trava o processamento dos outros — e cada uma pode falhar/reter
independentemente.

📖 aprofundar em [sistemas/trino.md](sistemas/trino.md) (o que é `MERGE`
sobre Iceberg) e [sistemas/iceberg.md](sistemas/iceberg.md).

### 5. `build_gold`

Task única: recalcula os marts (`sql/gold/`) — métricas já prontas pro
dashboard, ex. duração média por tipo de processo — lendo a silver de
**todos** os tenants numa passada só (o particionamento por `tenant_id` no
Iceberg mantém os dados fisicamente separados mesmo processando junto).
Estratégia é `DELETE` + `INSERT` (full rebuild): simples e sempre correta,
com uma janela breve de "tabela vazia" que é aceitável no laboratório.

### 6. `publish_tenant` — uma instância por tenant, em paralelo

Para cada tenant, [stages/serving.py](../airflow/dags/stages/serving.py) lê
via Trino **apenas a partição daquele tenant** da gold, e copia (truncate +
insert) pro Postgres `serving_<tenant>` — **sem a coluna `tenant_id`**, que
não tem mais motivo de existir a essa altura (o database inteiro já é do
tenant). Em seguida dá `GRANT SELECT` na tabela para a role `svc_<tenant>`.
É aqui que o `tenant_id`, que nasceu na Fase 0 como prefixo de tópico,
**"morre"** — deixa de existir como dado explícito, virando a própria
identidade do database.

### 7. `isolation_check`

Última task, roda sempre: para cada tenant, conecta com a credencial
`svc_<tenant>` no **próprio** serving (tem que funcionar) e tenta conectar
com essa mesma credencial no serving de **todos os outros** tenants (tem que
falhar). Se qualquer conexão cruzada funcionar, a task falha e **derruba o
pipeline inteiro** — é a prova automatizada, a cada ciclo, de que a parede
de isolamento continua de pé.

**Resultado da Fase 1**: cada tenant tem, no seu próprio database Postgres
de serving, os marts atualizados — e só a credencial daquele tenant consegue
lê-los.

---

## Fase 2 — Consumo (manual, fora da DAG)

O Metabase é configurado **uma vez** (não faz parte da DAG): aponta pro
Postgres `serving-db`, usando a connection string de **um** tenant por vez
(ex. `serving_tenant_acme` com o usuário `svc_tenant_acme`). A partir daí,
cada vez que a DAG roda e atualiza o serving, os gráficos do Metabase
mostram dado fresco — sem que o Metabase saiba nada sobre CDC, lake ou
Trino: ele só enxerga um Postgres normal, já pronto.

Isso é **só demonstração**: em produção, quem lê o `serving_<tenant>` seria
o próprio backend do BPMS, autenticado como aquele tenant — não uma
ferramenta de BI genérica olhando todo mundo.

📖 aprofundar em [sistemas/metabase.md](sistemas/metabase.md)

---

## Linha do tempo resumida

```text
docker compose up
   │
   ├─ tenant-db: cria N databases + dados sintéticos ─────────────┐
   ├─ serving-db: cria N databases vazios + N roles ───────────── │ paralelo
   ├─ kafka sobe → connect sobe → connect-init registra N conectores
   │      (Debezium começa a capturar mudanças AGORA, direto pro Kafka)
   ├─ minio sobe → bucket criado → iceberg-catalog sobe → trino sobe
   └─ airflow sobe, carrega a DAG, fica esperando
              │
              ▼ (trigger manual ou cron de 5 em 5 min)
   lake_init → land_events → tenant_ids → silver_tenant ×N (paralelo)
              → build_gold → publish_tenant ×N (paralelo) → isolation_check
              │
              ▼
   Metabase (configurado 1x) lê serving_<tenant> e mostra o dashboard
```

## Onde estudar cada peça

| Sistema | Por que está na stack | Aprofundamento |
| --- | --- | --- |
| Postgres | Origem de cada tenant + serving + metadados do Airflow | [sistemas/postgres.md](sistemas/postgres.md) |
| Debezium | Captura mudanças (CDC) direto do log do banco | [sistemas/debezium.md](sistemas/debezium.md) |
| Kafka (+ Connect) | Transporta os eventos capturados até o lake | [sistemas/kafka.md](sistemas/kafka.md) |
| MinIO | Armazenamento de arquivos do lake (imita S3) | [sistemas/minio.md](sistemas/minio.md) |
| Iceberg | Formato de tabela sobre os arquivos (permite MERGE) | [sistemas/iceberg.md](sistemas/iceberg.md) |
| Trino | Motor SQL que executa MERGE e agregações | [sistemas/trino.md](sistemas/trino.md) |
| Airflow | Decide quando/como cada etapa roda | [sistemas/airflow.md](sistemas/airflow.md) |
| Metabase | Demonstração visual do resultado | [sistemas/metabase.md](sistemas/metabase.md) |
