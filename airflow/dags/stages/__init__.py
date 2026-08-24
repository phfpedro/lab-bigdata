"""Etapas do pipeline, uma por módulo, isoladas por contratos.

Cada módulo declara no docstring seu contrato de ENTRADA e de SAÍDA.
A regra do jogo: um módulo só conhece os contratos vizinhos — nunca a
implementação interna de outra etapa. Trocar a implementação de uma etapa
(ex.: Debezium → outro CDC, Trino → Athena, Postgres → outro serving) não
deve exigir mudanças nas demais, desde que o contrato seja mantido.

    config.py    → registro único de tenants/tabelas (todas as etapas leem)
    landing.py   → E2: tópicos CDC ─▶ bronze Iceberg (append-only)
    transform.py → E3: bronze ─▶ silver (MERGE) ─▶ gold (marts)
    serving.py   → E5: gold ─▶ databases serving_<tenant> + teste de isolamento

A orquestração (E4) é a DAG bpms_analytics.py, que apenas encadeia as etapas
e decide janelas/paralelismo — nenhuma regra de negócio vive lá.
"""
