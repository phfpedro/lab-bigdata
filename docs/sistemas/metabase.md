# Metabase — aprofundamento

> Visão geral rápida: [stack.md](../stack.md). Fluxo completo: [fluxo-completo.md](../fluxo-completo.md).

## O que é

Uma ferramenta de **BI (Business Intelligence)** open-source: conecta em um
banco de dados existente, deixa montar consultas visualmente (ou em SQL) e
transforma o resultado em gráficos e dashboards, sem escrever código de
frontend.

## Por que Metabase neste projeto (e o que ele NÃO representa)

Metabase **não faz parte da arquitetura de contratos** descrita em
[arquitetura.md](../arquitetura.md) — é rotulado ali como "E6 — consumo
(demonstração)" justamente porque seu papel é só **provar visualmente** que
o pipeline entrega valor: conectar, ver dado de verdade, virar gráfico.

Em produção, o consumidor real do `serving_<tenant>` seria o **backend do
próprio BPMS**, autenticado como o tenant logado — nunca uma ferramenta de
BI genérica compartilhada. O motivo de estar aqui é puramente
demonstrativo: sem ele, "o pipeline funciona" ficaria provado só por linhas
de log e `SELECT`s manuais, difícil de mostrar pra alguém não-técnico.

## Conceitos-chave para estudar

- **Metabase não sabe nada do pipeline**: ele enxerga um Postgres normal
  (`serving_<tenant>`), com tabelas já prontas (`process_summary_by_type`,
  `daily_activity`) — não tem ideia de que existe CDC, Kafka, Iceberg ou
  Trino por trás. Essa "ignorância" é o próprio objetivo da camada de
  serving: entregar dado simples o bastante pra qualquer ferramenta comum
  consumir sem esforço.
- **Uma conexão = um tenant**: como o isolamento é por database +
  credencial (ver [sistemas/postgres.md](postgres.md)), cada conexão
  configurada no Metabase usa a credencial de **um único** tenant. Não
  existe (nem deveria existir) uma conexão "admin" que veja todos — a mesma
  parede de isolamento da E5 se aplica aqui.
- **`MB_DB_FILE`**: o Metabase guarda sua própria configuração (conexões
  salvas, usuários, dashboards) num banco próprio — aqui, arquivo local
  (H2) em vez de Postgres externo, simplificação de laboratório.

## Onde ver isso rodando neste projeto

- Serviço `metabase` em [docker-compose.yml](../../docker-compose.yml).
- Passo a passo de configuração manual (só na 1ª vez): [README.md](../../README.md).

## Para explorar na prática (lab rodando)

Siga o passo a passo do [README.md](../../README.md) — criar conta admin,
conectar em `serving_tenant_acme` com a connection string fornecida, criar
as duas perguntas (`process_summary_by_type`, `daily_activity`) e montar o
dashboard. Depois, tente repetir o processo criando uma segunda conexão
apontando para `serving_tenant_beta` com a credencial de outro tenant, para
ver na prática que são dados completamente diferentes, sem nenhuma
sobreposição.

## Caminho pra produção

Não faz parte do mapa PoC → produção em [arquitetura.md](../arquitetura.md)
justamente por ser só demonstração — o "produção" equivalente é a tela do
próprio BPMS, não uma tabela de mapeamento de tecnologia.
