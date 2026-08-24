# Kafka (+ Kafka Connect) — aprofundamento

> Visão geral rápida: [stack.md](../stack.md). Fluxo completo: [fluxo-completo.md](../fluxo-completo.md).

## O que é

Kafka é um **canal de mensagens distribuído**: programas publicam mensagens
em "tópicos", e outros programas leem essas mensagens, sem que publicador e
leitor se conheçam ou precisem estar online ao mesmo tempo — o Kafka guarda
a mensagem até alguém ler (ou até expirar, configurável). É o "correio" que
desacopla quem produz o dado (Debezium) de quem o consome (a task
`land_events` do Airflow).

**Kafka Connect** é um framework separado, mantido pelo mesmo projeto Kafka,
que hospeda **conectores** — plugins que empurram dados pra dentro do Kafka
(source connectors, como o Debezium) ou puxam dados pra fora (sink
connectors). Neste projeto ele só serve de "casa" pro Debezium rodar
([sistemas/debezium.md](debezium.md)) — o serviço `connect` no
`docker-compose.yml`.

## Por que Kafka neste projeto

Não há uma linha própria pra Kafka na tabela de decisões de
[arquitetura.md](../arquitetura.md) porque ele é consequência direta da
escolha de CDC via Debezium (E1): é o destino padrão dos eventos que o
Debezium produz, e o único acoplamento real entre extração e landing é o
**contrato de tópicos** (`<tenant>.public.<tabela>`, envelope Debezium
JSON) — não a tecnologia de mensageria em si.

## Conceitos-chave para estudar

- **Tópico**: uma "categoria" nomeada de mensagens. Aqui, um tópico por
  combinação tenant+tabela (ex. `tenant_acme.public.processes`).
- **Partição (do tópico)**: cada tópico é dividido em partições, que
  permitem paralelismo de leitura/escrita — mas também são a unidade de
  **ordem garantida** (mensagens numa mesma partição chegam na ordem em que
  foram escritas; entre partições diferentes, não há garantia). Neste
  laboratório cada tópico tem 1 partição só (baixo volume); em produção,
  tópicos de alto volume teriam várias.
- **Broker**: o processo que efetivamente guarda e serve as mensagens.
  Um cluster real tem vários brokers; aqui, só 1 (`kafka`, container único).
- **KRaft**: o modo mais recente do Kafka de gerenciar metadados do cluster
  **sem depender do ZooKeeper** (que era obrigatório em versões antigas).
  Este projeto já sobe nativamente em KRaft, um nó só fazendo os dois papéis
  (`broker,controller`).
- **Offset**: a posição de leitura de um consumer dentro de uma partição —
  "já li até a mensagem 400". É o que permite retomar a leitura de onde
  parou, mesmo depois de reiniciar.
- **Consumer group**: um "nome de time" que agrupa consumidores e faz o
  Kafka lembrar coletivamente até onde aquele time já leu cada partição.
  Neste projeto, a task `land_events` conecta como o grupo `lake-landing` —
  ver [airflow/dags/stages/landing.py](../../airflow/dags/stages/landing.py).
- **Commit manual de offset** (`enable.auto.commit: False`): a landing só
  confirma o offset **depois** de gravar os dados no lake — não antes. Se o
  processo cair no meio, na próxima execução ele relê a partir do último
  offset confirmado (podendo reler mensagens já gravadas, nunca perdê-las).
  Isso é o que dá o "at-least-once" citado em
  [arquitetura.md](../arquitetura.md#semânticas-importantes) — e por isso a
  silver precisa deduplicar (ver [sistemas/trino.md](trino.md)).
- **`auto.offset.reset: earliest`**: quando um consumer group é **novo**
  (nunca leu esse tópico antes), decide começar do início do tópico em vez
  de só das mensagens futuras — importante aqui porque o snapshot inicial
  do Debezium também vira mensagens no tópico, e não queremos perdê-las.

## Onde ver isso rodando neste projeto

- Config do broker: [docker-compose.yml](../../docker-compose.yml), serviço `kafka`.
- Config do Connect: mesmo arquivo, serviço `connect`.
- Quem consome: [airflow/dags/stages/landing.py](../../airflow/dags/stages/landing.py).
- Quem produz: o conector Debezium, ver [sistemas/debezium.md](debezium.md).

## Para explorar na prática (lab rodando)

```bash
# entrar no container do Kafka pra usar as ferramentas de linha de comando
docker compose exec kafka bash

# listar tópicos
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# ver o lag do consumer group da landing (quanto falta ler)
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group lake-landing

# ler as mensagens cruas de um tópico (envelope Debezium em JSON)
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic tenant_acme.public.processes --from-beginning --max-messages 5
```

## Caminho pra produção

Na AWS, o par Kafka + Debezium vira MSK + MSK Connect — a **mesma peça**,
só que gerenciada (sem operar broker/patch/escala manualmente). Self-hosted,
continua Kafka + Debezium em containers/k8s. A alternativa descartada foi
substituir tudo por AWS DMS (troca a extração inteira, não é drop-in).
