# Apache Airflow — aprofundamento

> Visão geral rápida: [stack.md](../stack.md). Fluxo completo: [fluxo-completo.md](../fluxo-completo.md).
> Task a task da DAG deste projeto: [dag-tasks.md](../dag-tasks.md).

## O que é

Um **orquestrador de tarefas**: não processa dado nenhum sozinho — decide
**quando** cada etapa roda, **em que ordem**, cuida de retry quando algo
falha, e dá visibilidade (UI web, logs, histórico) de tudo isso. É o
"maestro", nunca o "instrumento" (analogia do [glossário](../glossario.md)).

## Por que Airflow, e por que essa DAG específica

Da tabela de decisões em [arquitetura.md](../arquitetura.md) (E4):

| Alternativa | Por que não |
| --- | --- |
| Uma DAG única "tudo junto" (todos os tenants processados como um bloco) | Falha de um tenant (ex. evento CDC malformado) travaria/refaria o ciclo inteiro dos outros |
| Uma DAG por tenant (200+ DAGs) | Multiplica a operação (200+ agendamentos, 200+ pontos de monitoramento) sem necessidade — a maior parte do trabalho (landing, gold) já é feita bem numa passada só |

A solução adotada é **híbrida**: tasks que processam todos os tenants juntos
onde isso é seguro e eficiente (`land_events`, `build_gold`), e tasks que se
multiplicam **uma por tenant** exatamente onde falha isolada importa
(`silver_tenant`, `publish_tenant`) — usando **dynamic task mapping**, não
200 DAGs escritas à mão.

## Conceitos-chave para estudar

- **DAG (Directed Acyclic Graph)**: o "roteiro" — uma lista de tasks com
  setas de dependência (`>>`), sem ciclos. A definição completa deste
  projeto: [airflow/dags/bpms_analytics.py](../../airflow/dags/bpms_analytics.py).
- **Schedule (`*/5 * * * *`)**: expressão cron — a cada 5 minutos. Este é um
  desenho de **micro-batch**: não é streaming puro (evento a evento, latência
  de milissegundos) nem batch diário — é um meio-termo deliberado.
- **`catchup=False`**: ao subir a DAG pela primeira vez, o Airflow **não**
  tenta rodar retroativamente todos os ciclos que "deveriam" ter acontecido
  desde `start_date` — só passa a rodar dali pra frente.
- **`max_active_runs=1`**: garante que nunca há dois ciclos do pipeline
  rodando ao mesmo tempo — evita duas execuções concorrentes brigando pelos
  mesmos dados.
- **Dynamic task mapping** (`.expand(tenant_id=tenants)`): o mecanismo que
  faz uma task se multiplicar automaticamente, uma cópia por item de uma
  lista conhecida só em tempo de execução (aqui, a lista de tenants lida do
  registro). É o que permite adicionar um tenant novo em
  `config/tenants.yml` sem tocar em nenhum código da DAG.
- **`max_active_tis_per_dagrun`**: limita quantas cópias de uma task
  mapeada rodam em paralelo **dentro do mesmo ciclo** (aqui, 2 — ver
  `TENANT_PARALLELISM` no código) — protege o Trino e o serving de rajadas
  quando há muitos tenants. Em produção, o comentário no código já aponta a
  evolução natural: **Pools** do Airflow, que são compartilháveis entre
  DAGs e ajustáveis pela UI sem redeploy.
- **`retries` / `retry_delay`**: comportamento default de toda task nesta
  DAG — até 2 tentativas, 1 minuto de espera entre elas.
- **`data_interval_start`**: o início da "janela de tempo" que aquele ciclo
  representa — usado pela task `silver_tenant` para calcular, junto com o
  lookback, qual fatia da bronze processar.
- **XCom (implícito)**: o mecanismo por trás de tasks passarem valores umas
  às outras (aqui, a lista de `tenant_ids()` indo da task `tenant_ids` pras
  tasks mapeadas) — não aparece explicitamente no código porque o SDK
  moderno do Airflow (`@task`, retorno de função) abstrai isso.
- **Separação DAG vs. `stages/`**: o arquivo da DAG é **só coordenação**
  (ordem, janelas, paralelismo) — nenhuma regra de negócio. Toda lógica real
  vive nos módulos em `airflow/dags/stages/` e no SQL versionado em
  `airflow/dags/sql/`. Essa separação é o que permite trocar o motor de
  execução (Trino → outro) sem tocar na DAG.

## Onde ver isso rodando neste projeto

- [airflow/dags/bpms_analytics.py](../../airflow/dags/bpms_analytics.py) — a DAG.
- [airflow/dags/stages/](../../airflow/dags/stages/) — a lógica por trás de cada task.
- Config do serviço (executor, autenticação simplificada de lab, variáveis
  de ambiente compartilhadas com os `stages/`): serviço `airflow` em
  [docker-compose.yml](../../docker-compose.yml).

## Para explorar na prática (lab rodando)

1. Abra <http://localhost:8080> (sem login, modo lab).
2. Na DAG `bpms_analytics`, clique em **Graph** para ver visualmente o
   `silver_tenant ×N` e `publish_tenant ×N` se abrindo em paralelo.
3. Clique numa task individual → **Logs** — cada `print()` dos módulos em
   `stages/` aparece ali (ex. `"silver: processes atualizada para
   tenant_acme"`).
4. **Trigger DAG** manualmente a qualquer momento em vez de esperar o cron
   de 5 minutos.

## Caminho pra produção

Na AWS, vira **MWAA** (Airflow gerenciado) — a mesma DAG roda sem alteração.
Self-hosted, continua Airflow, tipicamente num cluster Kubernetes em vez de
`command: standalone` num container único.
