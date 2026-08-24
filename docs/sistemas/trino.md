# Trino — aprofundamento

> Visão geral rápida: [stack.md](../stack.md). Fluxo completo: [fluxo-completo.md](../fluxo-completo.md).

## O que é

Um **motor de consultas SQL distribuído**: recebe SQL, planeja a execução e
busca dados de uma ou mais fontes ("conectores" de catálogo — Iceberg, mas
também poderia ser Postgres, MySQL, etc. no mesmo cluster). Diferente de um
banco de dados tradicional, o Trino **não guarda dado nenhum** — ele só
consulta e processa o que está armazenado em outro lugar (aqui, os arquivos
Iceberg no MinIO). É a "calculadora" da stack.

## Por que Trino neste projeto

Da tabela de decisões em [arquitetura.md](../arquitetura.md) (E3):

| Alternativa | Por que não |
| --- | --- |
| Spark | Também resolveria, mas é uma stack mais pesada (JVM + cluster de execução distinto) para o volume deste laboratório; Trino é mais simples de operar self-hosted |
| DuckDB + PyIceberg | Leve e ótimo para exploração, mas não é feito pra ser um serviço always-on multi-usuário como o pipeline precisa |

O motivo decisivo, porém, é estratégico: Trino fala **o mesmo dialeto SQL**
que o Amazon **Athena** (que é, por baixo, um serviço gerenciado baseado em
Trino/Presto). Isso significa que todo SQL escrito neste projeto
(`dags/sql/`) migra para produção na AWS **quase 1:1**, só trocando o motor
de execução — sem reescrever a lógica de negócio.

## Conceitos-chave para estudar

- **Motor stateless / desacoplado do storage**: Trino não é dono dos dados
  — pode ser desligado e religado sem perder nada, porque tudo que importa
  (os arquivos, os metadados) vive no Iceberg/MinIO. Isso é o que permite
  trocá-lo por Athena em produção sem migrar dado nenhum.
- **Catálogo (no sentido do Trino)**: uma configuração que diz ao Trino
  "aqui tem uma fonte de dados chamada `iceberg`, veja em
  `platform/trino/catalog/iceberg.properties`". Um cluster Trino pode ter
  vários catálogos simultâneos (ex. um Iceberg, um Postgres) e fazer JOIN
  entre eles — não usado aqui, mas é um recurso central do Trino.
- **`MERGE INTO`**: como Trino executa merges sobre tabelas Iceberg — ver o
  SQL real, com deduplicação por `ROW_NUMBER() OVER (...)`, em
  [sql/silver/processes.sql](../../airflow/dags/sql/silver/processes.sql).
  Vale estudar essa query linha a linha: ela é o coração da transformação
  bronze→silver.
- **`json_extract_scalar` / `from_iso8601_timestamp`**: funções do Trino
  usadas pra extrair campos tipados de dentro do JSON cru salvo na bronze —
  é assim que a bronze "sem schema" vira silver tipada, sem precisar de uma
  etapa de parsing separada em outra linguagem.
- **Autocommit / statements separados**: o `DELETE` + `INSERT` da gold
  (ver [sql/gold/process_summary_by_type.sql](../../airflow/dags/sql/gold/process_summary_by_type.sql))
  roda como dois comandos distintos, não uma transação — decisão consciente
  de simplicidade que abre uma janela breve de "tabela vazia", documentada
  como pendência em [arquitetura.md](../arquitetura.md).
- **Cliente Python (`trino` package)**: como o Airflow fala com o Trino —
  uma conexão DBAPI normal, ver
  [airflow/dags/stages/transform.py](../../airflow/dags/stages/transform.py).

## Onde ver isso rodando neste projeto

- Configuração do catálogo: `platform/trino/catalog/` (montado como volume
  no serviço `trino`).
- Quem executa SQL contra o Trino: [airflow/dags/stages/transform.py](../../airflow/dags/stages/transform.py)
  (silver e gold) e [airflow/dags/stages/serving.py](../../airflow/dags/stages/serving.py) (leitura pro publish).
- Todo o SQL de negócio, versionado: [airflow/dags/sql/](../../airflow/dags/sql/).

## Para explorar na prática (lab rodando)

```bash
# CLI do Trino, dentro do próprio container
docker compose exec trino trino

# no prompt do Trino:
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SELECT * FROM iceberg.silver.processes LIMIT 10;
SELECT * FROM iceberg.gold.process_summary_by_type;
EXPLAIN SELECT * FROM iceberg.silver.processes WHERE tenant_id = 'tenant_acme';
```

A UI web do Trino (histórico de queries, planos de execução) também fica
disponível — vale abrir e observar uma execução real de `MERGE` enquanto a
DAG roda.

## Caminho pra produção

Na AWS, Trino vira **Athena** (mesma família/dialeto — migração quase 1:1
do SQL). Self-hosted, continua um cluster Trino (múltiplos nós em vez de
1 container).
