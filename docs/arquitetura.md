# Arquitetura — contratos, decisões e mapeamento pra produção

Ver [../NOTES.md](../NOTES.md). Este
documento é a referência de arquitetura: como as etapas se encaixam, por que
cada peça foi escolhida, e o que muda ao ir pra produção.

## O princípio: arquitetura por contratos

Cada etapa é uma caixa com contrato de **entrada** e **saída** explícitos
(documentados no docstring/cabeçalho de cada módulo). Uma etapa só conhece os
contratos vizinhos — nunca a implementação das outras. Resultado: qualquer
etapa pode ser trocada (ex.: Debezium → outro CDC, Trino → Athena) sem tocar
nas demais.

```text
┌─ E0 ORIGEM ───────────────────────────────────────────────────────────────┐
│ tenant-db: 1 database por tenant (simula o prod; sem coluna tenant_id)    │
├─ contrato: bancos Postgres com wal_level=logical ─────────────────────────┤
┌─ E1 EXTRAÇÃO ─────────────────────────────────────────────────────────────┐
│ Debezium + Kafka (KRaft 1 nó), 1 conector por tenant                      │
│ platform/connect/register-connectors.sh                                   │
├─ contrato: tópicos `<tenant>.public.<tabela>`, envelope Debezium JSON ────┤
┌─ E2 LAKE ─────────────────────────────────────────────────────────────────┐
│ MinIO + Iceberg REST catalog; landing agnóstica de schema                 │
│ stages/landing.py                                                         │
├─ contrato: iceberg.bronze.cdc_events (append-only, part. tenant + dia) ───┤
┌─ E3 TRANSFORMAÇÃO ────────────────────────────────────────────────────────┐
│ Trino: MERGE (silver) e agregações (gold); SQL versionado em dags/sql/    │
│ stages/transform.py                                                       │
├─ contrato: iceberg.silver.<tabela> (estado atual) e iceberg.gold.<mart> ──┤
┌─ E4 ORQUESTRAÇÃO ─────────────────────────────────────────────────────────┐
│ Airflow, DAG híbrida: landing única → silver ×N (mapping) → gold única    │
│ airflow/dags/bpms_analytics.py                                            │
├─ contrato: ordem, janelas, retry por tenant, paralelismo ─────────────────┤
┌─ E5 SERVING ──────────────────────────────────────────────────────────────┐
│ Postgres: 1 DATABASE por tenant + role exclusiva + teste de isolamento    │
│ stages/serving.py + platform/serving-db/init-serving.sh                   │
├─ contrato: serving_<tenant> com credencial que só alcança aquele tenant ──┤
┌─ E6 CONSUMO (demonstração) ───────────────────────────────────────────────┐
│ Metabase, plugado na serving com a credencial de UM tenant por vez        │
│ NÃO é o contrato de produção — só prova visualmente o valor do pipeline   │
└─ o consumo real seria o backend do BPMS lendo a serving_<tenant> do user ─┘

  Na gaveta: E7 (transversais: observabilidade, qualidade de dados, custos,
  segurança fim-a-fim)
```

## Decisões (e por quê)

| Etapa | Decisão | Alternativas consideradas | Motivo |
| --- | --- | --- | --- |
| E1 | CDC Debezium + Kafka, enxuto | Full load; watermark; changed-set via log de app; AWS DMS | Captura updates/deletes near real-time; forma mais simples da stack padrão para o time avaliar |
| E2 | Iceberg (REST catalog) | Parquet puro; Delta Lake | CDC pede `MERGE`; time travel nativo; na AWS vira Glue Catalog/S3 com suporte de Athena |
| E3 | Trino | Spark; DuckDB+PyIceberg | Tudo SQL; mesma família/dialeto do Athena (migração ~1:1) e operável self-hosted — bom nos dois futuros de infra |
| E4 | Airflow, DAG híbrida | DAG única "tudo junto"; 1 DAG por tenant | Falha/retry isolados por tenant onde importa (silver/publish) sem multiplicar operação ×200 |
| E5 | 1 database por tenant | Schema por tenant + RLS; consulta direta no lake | Tenants concorrentes: parede **estrutural** (conexão não alcança outro database) > parede de configuração |

Multi-tenant em números: o registro em `config/tenants.yml` é a fonte única —
adicionar um tenant provisiona banco de origem (lab), conector CDC, tasks
mapeadas no Airflow e serving. Nada é listado duas vezes.

> 📖 Fluxo completo em ordem cronológica (o que acontece, na ordem em que
> acontece, do `docker compose up` ao dashboard): [fluxo-completo.md](fluxo-completo.md)
>
> 📖 Documentação task a task da DAG (o que cada uma faz, com exemplos):
> [dag-tasks.md](dag-tasks.md)
>
> 📖 Dicionário de termos (linguagem simples, para quem não é da área):
> [glossario.md](glossario.md)
>
> 📖 Referência rápida das tecnologias (nome → o que é → o que faz aqui):
> [stack.md](stack.md)
>
> 📖 Aprofundamento por sistema (o que é, por que foi escolhido, conceitos
> pra estudar, como explorar no lab): [sistemas/](sistemas/)

## Estrutura do repositório

```text
config/                       # FONTE ÚNICA: tenants e tabelas
  tenants.yml                 #   registro de tenants (id = nome do database)
  tables.yml                  #   tabelas capturadas por CDC
platform/                     # infraestrutura por etapa
  tenant-db/                  #   E0: init + schema BPMS de exemplo
  connect/                    #   E1: registro dos conectores Debezium
  trino/catalog/              #   E3: catálogo Iceberg do Trino
  airflow/                    #   E4: imagem (deps por etapa)
  serving-db/                 #   E5: databases/roles por tenant
airflow/dags/
  bpms_analytics.py           # E4: a DAG (só coordenação)
  stages/                     # etapas como módulos com contrato no docstring
    config.py landing.py transform.py serving.py
  sql/                        # regra de negócio versionada (E3)
    init/ silver/ gold/
docker-compose.yml            # stack completa, comentada por etapa
```

## Semânticas importantes

- **At-least-once + idempotência**: a landing só confirma offsets após o
  append na bronze; a silver deduplica por PK (último evento por `lsn`/`ts_ms`
  vence). Reprocessar janelas não duplica nem corrompe — pré-requisito para
  retry e backfill.
- **Janela da silver**: intervalo do run com lookback largo
  (`SILVER_LOOKBACK_HOURS`, default 24h). Overlap é seguro (MERGE); o lookback
  cobre atrasos e reprocessos.
- **tenant_id**: não existe na origem (é o nome do database). Nasce na E1
  (prefixo do tópico), viaja pelo lake como coluna/partição e **morre na E5**
  — não é publicado no serving.
- **Isolamento em camadas**: partição física por tenant no lake (zona
  interna) → publish lê apenas a partição do tenant → database exclusivo →
  role exclusiva → `REVOKE CONNECT FROM PUBLIC` → teste automatizado.

## Mapa PoC → produção (AWS ou self-hosted)

| Peça no PoC | AWS gerenciado | Self-hosted |
| --- | --- | --- |
| MinIO | S3 | MinIO |
| Iceberg REST fixture | Glue Catalog | REST catalog (Polaris/Lakekeeper) |
| Trino (container) | Athena (mesmo dialeto) | Cluster Trino |
| Kafka + Debezium | MSK + Connect (mesma peça, gerenciada) — ou trocar tudo por DMS | Kafka + Debezium |
| Airflow standalone | MWAA | Airflow (k8s/compose) |
| Postgres serving | RDS | Postgres |

## Pendências conscientes (além das etapas na gaveta)

- Credenciais hardcoded (lab) → secrets manager em produção.
- Schema evolution da origem: coluna nova exige atualizar o SQL da silver
  (o contrato da bronze absorve sem quebra, pois o payload é JSON).
- Manutenção Iceberg (compaction/expire de snapshots) → DAG de manutenção.
- Gold com DELETE+INSERT em statements separados (janela breve de vazio) →
  staging/branching em produção.
- 1 slot de replicação por tenant: monitorar lag/disco no primário (slot
  abandonado segura WAL) — ponto de atenção real para os 200+.
