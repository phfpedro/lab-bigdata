"""E5 — Serving: publica a gold no banco exclusivo de cada tenant.

Contrato de ENTRADA (vem da E3 — transformação):
    iceberg.gold.<mart> particionado por tenant_id.

Contrato de SAÍDA (consumido pela E6 — aplicação/BI, na gaveta):
    Database Postgres `serving_<tenant>` — um POR tenant — contendo os marts
    SEM a coluna tenant_id, acessível somente pela role exclusiva
    `svc_<tenant>`. A parede é estrutural: conexão em um database não
    alcança os demais (CONNECT revogado do PUBLIC no provisionamento).

Regras de segurança deste módulo:
  1. A leitura da gold filtra por partição de tenant — o publish de um tenant
     é INCAPAZ de ler dados de outro.
  2. A coluna tenant_id nem sequer é publicada.
  3. isolation_check() roda após todo publish: valida positivamente (a role
     acessa o próprio serving) e negativamente (a role NÃO conecta em nenhum
     serving alheio). Qualquer brecha derruba o pipeline.
"""

import os

import psycopg2
from psycopg2.extras import execute_values
import trino

from stages import config

# Marts publicados: colunas na ordem (gold → serving). tenant_id fica de fora.
MARTS: dict[str, list[tuple[str, str]]] = {
    "process_summary_by_type": [
        ("process_type", "text"),
        ("total_processes", "bigint"),
        ("open_processes", "bigint"),
        ("closed_processes", "bigint"),
        ("canceled_processes", "bigint"),
        ("avg_duration_hours", "double precision"),
        ("computed_at", "timestamptz"),
    ],
    "daily_activity": [
        ("activity_date", "date"),
        ("processes_opened", "bigint"),
        ("processes_closed", "bigint"),
        ("protocols_logged", "bigint"),
        ("computed_at", "timestamptz"),
    ],
}


def _trino_cursor():
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "pipeline"),
    ).cursor()


def _serving_conn(dbname: str, user: str | None = None, password: str | None = None):
    """Conexão no serving. Sem user/password explícitos usa a credencial
    ADMINISTRATIVA do pipeline (que publica); com eles, simula um tenant."""
    return psycopg2.connect(
        host=os.environ.get("SERVING_HOST", "serving-db"),
        port=int(os.environ.get("SERVING_PORT", "5432")),
        dbname=dbname,
        user=user or os.environ.get("SERVING_ADMIN_USER", "postgres"),
        password=password or os.environ.get("SERVING_ADMIN_PASSWORD", "serving"),
        connect_timeout=5,
    )


def publish_tenant(tenant_id: str) -> None:
    """Carrega os marts do tenant no database serving_<tenant> (truncate+load)."""
    if tenant_id not in config.tenant_ids():
        raise ValueError(f"tenant desconhecido: {tenant_id!r}")
    role = f"svc_{tenant_id}"
    tcur = _trino_cursor()
    pg = _serving_conn(f"serving_{tenant_id}")
    try:
        with pg, pg.cursor() as cur:
            for mart, columns in MARTS.items():
                names = [name for name, _ in columns]
                # leitura JÁ filtrada pela partição do tenant (parametrizada)
                tcur.execute(
                    f"SELECT {', '.join(names)} FROM iceberg.gold.{mart} WHERE tenant_id = ?",
                    (tenant_id,),
                )
                rows = tcur.fetchall()

                ddl = ", ".join(f"{name} {sqltype}" for name, sqltype in columns)
                cur.execute(f"CREATE TABLE IF NOT EXISTS {mart} ({ddl})")
                cur.execute(f"TRUNCATE {mart}")
                if rows:
                    execute_values(
                        cur, f"INSERT INTO {mart} ({', '.join(names)}) VALUES %s", rows
                    )
                cur.execute(f"GRANT SELECT ON {mart} TO {role}")
                print(f"publish {tenant_id}: {len(rows)} linhas em {mart}")
    finally:
        pg.close()


def isolation_check() -> None:
    """Prova automatizada do isolamento — roda a cada ciclo do pipeline.

    Positivo: a role de cada tenant lê os marts do PRÓPRIO serving.
    Negativo: a mesma role é barrada ao conectar no serving de QUALQUER
    outro tenant. Uma conexão cruzada bem-sucedida derruba o pipeline.
    """
    tenants = config.tenant_ids()
    breaches: list[str] = []

    for tenant_id in tenants:
        role, password = f"svc_{tenant_id}", f"pw_{tenant_id}"  # convenção do lab

        # 1) acesso legítimo funciona
        own = _serving_conn(f"serving_{tenant_id}", user=role, password=password)
        with own, own.cursor() as cur:
            for mart in MARTS:
                cur.execute(f"SELECT count(*) FROM {mart}")
                print(f"isolation {tenant_id}: {mart} = {cur.fetchone()[0]} linhas")
        own.close()

        # 2) acesso cruzado é estruturalmente impossível
        for other in tenants:
            if other == tenant_id:
                continue
            try:
                crossed = _serving_conn(f"serving_{other}", user=role, password=password)
            except psycopg2.OperationalError:
                continue  # esperado: "permission denied for database"
            crossed.close()
            breaches.append(f"{role} conectou em serving_{other}")

    if breaches:
        raise RuntimeError(f"FALHA DE ISOLAMENTO ENTRE TENANTS: {breaches}")
    print(f"isolation: OK — {len(tenants)} tenants, nenhuma conexão cruzada possível")
