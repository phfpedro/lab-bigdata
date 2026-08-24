# Stack — o que é cada peça

Nome, definição em 1 linha, e o que ela faz especificamente neste projeto —
sem explicar conceito (isso está no [glossário](glossario.md)). Para a visão
de arquitetura e os contratos entre etapas, ver [arquitetura.md](arquitetura.md).
Para o fluxo em ordem cronológica, ver [fluxo-completo.md](fluxo-completo.md).
Para se aprofundar em cada tecnologia (por quê, conceitos-chave, como
explorar no lab), ver [sistemas/](sistemas/).

| Tecnologia | O que é | O que faz nesta stack |
| --- | --- | --- |
| **Postgres** | Banco de dados relacional (SQL) | Banco de origem de cada tenant, banco final da serving, e banco de metadados do Airflow |
| **Debezium** | Ferramenta de CDC — lê o log interno do banco e vira cada mudança em evento | Vigia os bancos de origem e publica todo INSERT/UPDATE/DELETE no Kafka |
| **Kafka** | Canal de mensagens distribuído — guarda e entrega eventos | Recebe os eventos do Debezium e os mantém até a task `land_events` consumir |
| **Kafka Connect** | Framework que hospeda plugins de conectores (como o Debezium) | É onde o Debezium roda de fato, dentro do nosso stack |
| **MinIO** | Armazenamento de objetos compatível com a API do S3 | Simula o S3 no laboratório; guarda os arquivos físicos do lake |
| **Iceberg** | Formato de tabela para arquivos (permite MERGE, versionamento, partição) | Organiza bronze/silver/gold como tabelas, mesmo sendo arquivos no MinIO |
| **Trino** | Motor de consultas SQL distribuído | Executa os `MERGE` (bronze→silver) e as agregações (silver→gold) |
| **Airflow** | Orquestrador de tarefas | Decide quando e em que ordem cada etapa roda, cuida de retry/falha |
| **Metabase** | Ferramenta de BI (dashboards) | Conecta na serving e transforma os marts em gráfico — só demonstração |
