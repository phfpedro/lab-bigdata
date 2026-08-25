# Debezium — aprofundamento

> Visão geral rápida: [stack.md](../stack.md). Fluxo completo: [fluxo-completo.md](../fluxo-completo.md).

## O que é

Uma plataforma open-source de **CDC (Change Data Capture)**: em vez de
ficar perguntando ao banco "o que mudou desde a última vez?" (uma técnica
chamada *watermark*, baseada em colunas tipo `updated_at`), o Debezium se
conecta como um **replica** e lê diretamente o log interno de transações do
banco — no caso do Postgres, o WAL (ver [sistemas/postgres.md](postgres.md)).
Cada `INSERT`, `UPDATE` e `DELETE` vira um evento, publicado no Kafka, quase
em tempo real (segundos, não minutos).

Roda como um **conector** dentro do Kafka Connect (ver
[sistemas/kafka.md](kafka.md)) — o Debezium não é um serviço próprio no
`docker-compose.yml`, é um plugin instalado dentro da imagem `connect`.

## Por que CDC (e por que Debezium) neste projeto

Da tabela de decisões em [arquitetura.md](../arquitetura.md) (E1):

| Alternativa | Por que não |
| --- | --- |
| Full load (recarregar a tabela inteira a cada ciclo) | Não escala, reprocessa dado que não mudou |
| Watermark (`WHERE updated_at > última_leitura`) | Não enxerga `DELETE`s, exige coluna de auditoria em toda tabela |
| Changed-set via log de aplicação | Exige instrumentar cada sistema de origem — não dá pra fazer isso em 200+ tenants de um produto já existente |
| AWS DMS | Fecha em nuvem específica; Debezium é a peça padrão que roda igual self-hosted e dentro do MSK Connect gerenciado (ver mapa pra produção) |

CDC via log é a única abordagem que captura update/delete de forma completa
e sem tocar no código da aplicação de origem — só precisa ligar uma flag no
banco (`wal_level=logical`).

## Conceitos-chave para estudar

- **Snapshot inicial vs. streaming**: quando um conector é criado
  (`snapshot.mode: initial` na config deste projeto), o Debezium primeiro
  faz uma cópia completa da tabela como ela está **agora** (o snapshot),
  e só depois passa a streamar as mudanças a partir daquele ponto no WAL.
  Sem o snapshot, um tenant cadastrado hoje começaria "vazio" — só veria
  mudanças futuras, nunca o histórico existente.
- **Envelope Debezium**: o formato de cada evento publicado no Kafka.
  Contém `before` (estado antigo da linha, ou `null` num INSERT), `after`
  (estado novo, ou `null` num DELETE), `op` (`c`=create, `u`=update,
  `d`=delete, `r`=read/snapshot) e `source` (metadados como `lsn`, tabela,
  banco). Ver o parser real em
  [airflow/dags/stages/landing.py](../../airflow/dags/stages/landing.py).
- **`topic.prefix`**: o nome que o conector usa como prefixo de todos os
  tópicos que ele cria. Neste projeto é o **próprio nome do tenant** — é
  literalmente aqui que o conceito de `tenant_id` nasce no pipeline (a
  origem não tem essa coluna; o tenant é o database inteiro).
- **`table.include.list`**: allowlist de quais tabelas o conector deve
  vigiar — vem de [config/tables.yml](../../config/tables.yml), a mesma
  fonte que a landing e a silver usam, para nunca haver divergência.
- **Replication slot por conector**: cada conector Debezium cria seu
  próprio slot no Postgres de origem (`slot.name: cdc_<tenant>`), e como
  slots são por instância (não por database), o nome precisa ser único —
  daí levar o tenant no nome.
- **`decimal.handling.mode` / `tombstones.on.delete`**: detalhes de
  serialização configurados neste projeto por simplicidade
  (decimais como `double`, sem tombstone de delete) — vale olhar a
  [documentação oficial do conector Postgres](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
  para entender o que cada opção realmente controla em produção.

## Onde ver isso rodando neste projeto

- [platform/connect/register-connectors.sh](../../platform/connect/register-connectors.sh) — registra 1 conector por tenant via API REST do Kafka Connect.
- Payload do conector, statement por statement, comentado no próprio script.

## Para explorar na prática (com o ambiente rodando)

```bash
# listar conectores registrados
curl -s http://localhost:8083/connectors | jq

# status de um conector específico (rodando? snapshot completo?)
curl -s http://localhost:8083/connectors/cdc-tenant_acme/status | jq

# ver a config completa que foi enviada
curl -s http://localhost:8083/connectors/cdc-tenant_acme/config | jq

# forçar uma mudança na origem e ver o evento aparecer (combine com sistemas/kafka.md)
psql -h localhost -p 5433 -U postgres -d tenant_acme \
  -c "UPDATE processes SET status='closed' WHERE id=1;"
```

## Caminho pra produção

Na AWS, o Debezium continua sendo a mesma peça, só que hospedado no MSK
Connect (gerenciado) em vez de um container próprio — não muda a lógica.
A alternativa descartada foi trocar tudo por AWS DMS, que funciona mas é uma
reescrita da extração inteira, não uma migração incremental.
