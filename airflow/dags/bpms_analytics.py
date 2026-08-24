"""E4 — Orquestração: DAG única, desenho híbrido (decisão da etapa 4).

    lake_init ─▶ landing ─▶ silver (×N tenants) ─▶ gold ─▶ publish (×N) ─▶ isolation
                            └── dynamic task mapping ──┘   └── mapping ──┘

  - landing : task ÚNICA — o Kafka entrega todos os tenants misturados nos
              tópicos; a bronze separa por partição ao gravar.
  - silver  : task POR TENANT (dynamic task mapping) — falha e retry isolados;
              o tenant com evento podre não trava os outros. Tenant novo no
              registro (config/tenants.yml) entra sozinho no próximo ciclo.
  - gold    : task única — o Trino agrega todos os tenants numa passada
              (particionado por tenant_id).
  - publish : task POR TENANT — grava no serving exclusivo.
  - isolation: guarda final — prova que nenhuma credencial cruza tenants.

Este arquivo é SÓ coordenação: janelas, ordem, paralelismo e retry.
Nenhuma regra de negócio aqui — ela vive em stages/ e dags/sql/ (contratos).
"""

from datetime import datetime, timedelta, timezone

import pendulum
from airflow.sdk import dag, task

from stages import config, landing, serving, transform

# Quantos tenants processar em paralelo nas tasks mapeadas. Protege o Trino
# e o serving de rajadas com N tenants; em produção, preferir Pools do
# Airflow (visíveis/ajustáveis pela UI, compartilháveis entre DAGs).
TENANT_PARALLELISM = 2


@dag(
    schedule="*/5 * * * *",  # micro-batch (decisão da etapa 4) expressão cron
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), # A partir de quando essa DAG passa a existir no calendário do Airflow
    catchup=False, # Não tente "recuperar" execuções que teoricamente deveriam ter rodado no passado
    max_active_runs=1,  # um ciclo por vez: sem execuções concorrentes
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)}, # Se uma task falhar, tente de novo até 2 vezes, esperando 1 minuto entre tentativas
    tags=["bpms", "multi-tenant", "cdc", "iceberg", "trino"], # Só etiquetas visuais, para organizar/filtrar na tela do Airflow
)
def bpms_analytics():
    @task
    def lake_init() -> None:
        """DDL idempotente do lake (schemas + tabelas silver/gold)."""
        transform.lake_init()

    @task
    def land_events() -> int:
        """E2: drena os tópicos CDC e aterrissa na bronze (todos os tenants)."""
        return landing.land()

    @task
    def tenant_ids() -> list[str]:
        """Registro único → alimenta as tasks mapeadas por tenant."""
        return config.tenant_ids()

    @task(max_active_tis_per_dagrun=TENANT_PARALLELISM)
    def silver_tenant(tenant_id: str, data_interval_start=None) -> None:
        """E3: MERGE dos eventos do tenant na silver.

        A janela (responsabilidade da orquestração) é o intervalo do run com
        lookback largo — o MERGE idempotente torna o overlap seguro, e o
        lookback cobre atrasos de landing e reprocessamentos manuais.
        """
        import os

        lookback = timedelta(hours=int(os.environ.get("SILVER_LOOKBACK_HOURS", "24")))
        start = data_interval_start or datetime.now(timezone.utc)
        since_iso = (start - lookback).isoformat()
        transform.silver_for_tenant(tenant_id, since_iso)

    @task
    def build_gold() -> None:
        """E3: reconstrói os marts da gold (todos os tenants, particionado)."""
        transform.build_gold()

    @task(max_active_tis_per_dagrun=TENANT_PARALLELISM)
    def publish_tenant(tenant_id: str) -> None:
        """E5: publica os marts do tenant no database serving exclusivo."""
        serving.publish_tenant(tenant_id)

    @task
    def isolation_check() -> None:
        """E5: prova do isolamento — falha do pipeline se houver brecha."""
        serving.isolation_check()

    tenants = tenant_ids()
    silver = silver_tenant.expand(tenant_id=tenants)
    publish = publish_tenant.expand(tenant_id=tenants)

    lake_init() >> land_events() >> silver >> build_gold() >> publish >> isolation_check()


bpms_analytics()
