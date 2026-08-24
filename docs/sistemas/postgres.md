# Postgres — aprofundamento

> Visão geral rápida: [stack.md](../stack.md). Fluxo completo: [fluxo-completo.md](../fluxo-completo.md).

## O que é

Um banco de dados **relacional** (SQL): guarda dados em tabelas com colunas
tipadas, garante consistência (transações ACID) e é, de longe, o banco
open-source mais usado no mundo para carga transacional.

## Onde ele aparece neste projeto (é usado 3 vezes, com papéis diferentes)

1. **`tenant-db`** — a "origem", uma instância com **um database por
   tenant**. Simula o banco de produção real do BPMS. Sobe com
   `wal_level=logical`, uma flag que normalmente está desligada por padrão e
   que é o **pré-requisito técnico** para o CDC funcionar (ver abaixo).
2. **`serving-db`** — outra instância, separada da origem, com **um
   database vazio por tenant** que recebe só o resultado final (os marts).
   É nela que mora a parede de isolamento entre tenants.
3. **`postgres-airflow`** — um terceiro uso, sem relação com o negócio: é
   só onde o Airflow guarda seus próprios metadados (histórico de execuções,
   conexões, usuários). Poderia ser trocado por MySQL sem afetar o pipeline.

## Por que Postgres e não outro banco

Não foi exatamente uma "escolha" no sentido de comparar alternativas — é o
banco que o produto real (BPMS) já usa como origem, e reaproveitá-lo como
serving evita introduzir uma tecnologia nova só para a última milha. A
decisão real e documentada da arquitetura está em outro nível: **quantos
databases por tenant** (ver seção de isolamento abaixo).

## Conceitos-chave para estudar

- **WAL (Write-Ahead Log)**: antes de aplicar qualquer mudança nas tabelas,
  o Postgres escreve a intenção da mudança num log sequencial em disco. É
  esse log que o Debezium lê para saber o que mudou — sem ele, CDC não
  existiria.
- **`wal_level=logical`**: por padrão o WAL guarda só o necessário para
  recuperação de crash. O nível `logical` acrescenta informação suficiente
  para reconstruir cada mudança linha a linha (replicação lógica) — é o que
  o conector Debezium consome.
- **Replication slot**: um "marcador de leitura" no WAL, criado pelo
  Debezium (`slot.name` no conector), que garante que o Postgres não
  descarta WAL que ainda não foi lido pelo consumidor. Risco real de
  produção: um slot "esquecido" (conector caído) faz o WAL crescer sem
  limite até lotar o disco — ver a pendência anotada em
  [arquitetura.md](../arquitetura.md#pendências-conscientes-além-das-etapas-na-gaveta).
- **Publication**: no Postgres, define **quais tabelas** entram na
  replicação lógica (`publication.name` no conector, casado com
  `table.include.list`).
- **`CREATE DATABASE` vs. `CREATE SCHEMA`**: a decisão central de isolamento
  deste projeto (ver [arquitetura.md](../arquitetura.md), tabela de
  decisões, linha E5). Um `DATABASE` no Postgres é um espaço totalmente
  isolado: uma conexão abre para **um** database e fisicamente não enxerga
  os outros — nem com um bug de aplicação, a menos que a credencial em si
  tenha permissão. Um `SCHEMA` é só uma pasta lógica dentro do **mesmo**
  database — qualquer conexão pode, em tese, tentar consultar outro schema;
  o isolamento aí depende de RLS (row-level security) ou de permissões bem
  configuradas, ou seja, de configuração correta e nunca esquecida.
- **`REVOKE CONNECT ... FROM PUBLIC`**: por padrão, qualquer role do
  Postgres pode conectar em qualquer database. Este projeto revoga esse
  privilégio default e concede `CONNECT` só para a role dona daquele
  database — sem isso, criar um database por tenant não isolaria nada.
- **Roles e `GRANT`**: cada tenant tem uma role própria (`svc_<tenant>`)
  com `SELECT` concedido tabela a tabela, só depois que a tabela existe —
  nunca um `GRANT ALL`.

## Onde ver isso rodando neste projeto

- [platform/tenant-db/init-tenants.sh](../../platform/tenant-db/init-tenants.sh) — cria os databases de origem.
- [platform/serving-db/init-serving.sh](../../platform/serving-db/init-serving.sh) — cria databases + roles + revoga CONNECT.
- [platform/tenant-db/tenant_schema.sql](../../platform/tenant-db/tenant_schema.sql) — schema de exemplo (BPMS fictício) e dados sintéticos.
- [airflow/dags/stages/serving.py](../../airflow/dags/stages/serving.py) — quem escreve no serving e quem roda o teste de isolamento.

## Para explorar na prática (lab rodando)

```bash
# conectar na origem de um tenant
psql -h localhost -p 5433 -U postgres -d tenant_acme
# ver o WAL level
SHOW wal_level;
# ver os replication slots ativos
SELECT slot_name, active, restart_lsn FROM pg_replication_slots;

# conectar no serving como a role de UM tenant só
psql "postgresql://svc_tenant_acme:pw_tenant_acme@localhost:5434/serving_tenant_acme"
# tentar conectar no serving de outro tenant com essa credencial → deve falhar
psql "postgresql://svc_tenant_acme:pw_tenant_acme@localhost:5434/serving_tenant_beta"
```

## Caminho pra produção

RDS (Postgres gerenciado na AWS) nos dois papéis, ou self-hosted continua
Postgres — é a peça que menos muda no mapeamento pra produção (ver tabela em
[arquitetura.md](../arquitetura.md)). O que muda de verdade é gestão de
credenciais (secrets manager em vez de senha hardcoded) e automação do
provisionamento por tenant no onboarding.
