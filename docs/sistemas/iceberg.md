# Apache Iceberg — aprofundamento

> Visão geral rápida: [stack.md](../stack.md). Fluxo completo: [fluxo-completo.md](../fluxo-completo.md).

## O que é

Um **formato de tabela** para data lakes: uma camada de metadados por cima
de arquivos comuns (aqui, Parquet no MinIO) que faz esses arquivos se
comportarem como uma tabela de banco de dados de verdade — com schema,
partições, e o mais importante para este projeto, suporte a `UPDATE`,
`DELETE` e `MERGE`. Sem um formato de tabela, um "data lake" seria só uma
pilha de arquivos onde a única operação natural é `INSERT` (append) — nunca
corrigir ou remover um registro específico sem reescrever tudo.

O **catálogo Iceberg** (serviço `iceberg-catalog`, um "REST fixture" de
referência da Apache) é o componente que sabe, a qualquer momento, "qual é
a versão atual de cada tabela e quais arquivos a compõem" — é nele que o
Trino (e o PyIceberg da landing) perguntam antes de ler ou escrever.

## Por que Iceberg neste projeto

Da tabela de decisões em [arquitetura.md](../arquitetura.md) (E2):

| Alternativa | Por que não |
| --- | --- |
| Parquet puro (arquivos sem formato de tabela) | CDC produz updates/deletes; sem Iceberg não existe `MERGE`, teria que reescrever a tabela inteira a cada mudança |
| Delta Lake | Também resolveria o `MERGE`, mas o caminho de produção deste projeto mira Athena/Glue na AWS, e Iceberg é o formato nativamente suportado lá |

O ponto central: a silver precisa aplicar updates e deletes vindos do CDC
em cima de dados já gravados — e isso é exatamente o que um formato de
tabela como Iceberg resolve que um lake "cru" não resolve.

## Conceitos-chave para estudar

- **Snapshot**: cada mudança confirmada numa tabela Iceberg gera um novo
  snapshot — um "ponto no tempo" imutável da tabela inteira. Isso é a base
  do **time travel** (consultar a tabela como ela estava numa versão
  anterior) e também o motivo de tabelas Iceberg acumularem arquivos órfãos
  ao longo do tempo (ver "compaction" abaixo).
- **Metadata layer**: arquivos JSON/Avro (não visíveis "como tabela", mas
  visíveis como arquivos no MinIO) que descrevem schema, partições e qual
  conjunto de arquivos Parquet pertence a cada snapshot. É essa camada que
  faz uma pasta de Parquets virar uma "tabela" com `MERGE` de verdade.
  📖 explore isso em [sistemas/minio.md](minio.md), seção "para explorar
  na prática".
- **Particionamento** (`WITH (partitioning = ARRAY['tenant_id'])` no DDL
  deste projeto, ver [sql/init/00_lake_ddl.sql](../../airflow/dags/sql/init/00_lake_ddl.sql)):
  organiza fisicamente os arquivos por valor de uma coluna. Aqui, toda
  tabela do lake é particionada por `tenant_id` — o que faz "ler só os
  dados de um tenant" ser uma operação que nem toca nos arquivos dos outros
  tenants, e é a base física do isolamento descrito em
  [arquitetura.md](../arquitetura.md#semânticas-importantes).
- **`MERGE INTO`**: o comando que consolida insert/update/delete numa única
  operação declarativa. Ver o uso real, com dedup, em
  [sql/silver/processes.sql](../../airflow/dags/sql/silver/processes.sql).
- **Catálogo (REST catalog)**: o "índice" de tabelas — dado um nome como
  `iceberg.silver.processes`, resolve pra qual metadata JSON representa a
  versão atual. Neste projeto é o `apache/iceberg-rest-fixture`, um
  catálogo de referência simples; em produção AWS vira o **Glue Catalog**,
  ou self-hosted, algo como Polaris/Lakekeeper (ver mapa pra produção em
  [arquitetura.md](../arquitetura.md)).
- **Compaction / expire snapshots**: manutenção necessária em produção —
  compactar muitos arquivos pequenos em poucos grandes (melhora performance
  de leitura) e apagar snapshots antigos (libera espaço). Este laboratório
  **não** faz isso; é uma das pendências conscientes documentadas em
  [arquitetura.md](../arquitetura.md#pendências-conscientes-além-das-etapas-na-gaveta).

## Onde ver isso rodando neste projeto

- Criação da tabela bronze (schema Iceberg definido em código Python, via
  PyIceberg): [airflow/dags/stages/landing.py](../../airflow/dags/stages/landing.py).
- Criação das tabelas silver/gold (via DDL SQL, executado pelo Trino):
  [sql/init/00_lake_ddl.sql](../../airflow/dags/sql/init/00_lake_ddl.sql).
- Configuração do catálogo: serviço `iceberg-catalog` em
  [docker-compose.yml](../../docker-compose.yml), e o catálogo do Trino em
  `platform/trino/catalog/`.

## Para explorar na prática (lab rodando)

```sql
-- pelo Trino (ver sistemas/trino.md pra como conectar)
SELECT * FROM iceberg.silver."processes$snapshots";   -- histórico de snapshots
SELECT * FROM iceberg.silver."processes$files";        -- arquivos Parquet atuais
SELECT * FROM iceberg.silver.processes FOR VERSION AS OF <snapshot_id>;  -- time travel
```

## Caminho pra produção

Na AWS, o catálogo REST fixture vira **Glue Catalog** e o MinIO vira **S3**
— os dados continuam Iceberg, sem migração de formato. Self-hosted, o
catálogo vira algo mais robusto que o fixture de referência (Polaris,
Lakekeeper).
