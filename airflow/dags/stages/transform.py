"""E3 — Transformação: bronze → silver (MERGE) → gold (marts), via Trino.

Contrato de ENTRADA (vem da E2 — lake/landing):
    iceberg.bronze.cdc_events — eventos CDC crus com payload JSON
    (ver stages/landing.py).

Contrato de SAÍDA (consumido pela E5 — serving, e por consultas internas):
    iceberg.silver.<tabela> — estado ATUAL de cada tabela por tenant, com
        updates aplicados e deletes removidos (MERGE), tipado e particionado
        por tenant_id.
    iceberg.gold.<mart>     — métricas prontas para consumo, particionadas
        por tenant_id.

A regra de negócio vive nos arquivos SQL versionados em dags/sql/ — este
módulo só os executa. O motor é intercambiável: qualquer engine com MERGE
em Iceberg (Athena, Spark, ...) substitui o Trino reaproveitando os SQLs.

Idempotência: o MERGE da silver processa uma janela com lookback e deduplica
por PK (último evento por lsn/ts_ms vence) — reprocessar a mesma janela não
duplica nem corrompe. A gold é full rebuild (DELETE+INSERT) a partir da
silver: rodar duas vezes produz o mesmo resultado.
"""

import os
from pathlib import Path

import trino

from stages import config

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def _connection():
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "pipeline"),
    )


def _statements(sql: str) -> list[str]:
    """Divide um arquivo SQL em statements executáveis.

    Comentários de linha (--) são removidos ANTES do split por ';' — um ';'
    dentro de comentário não pode quebrar o parse. Premissa: nossos SQLs não
    usam '--' dentro de literais de string.
    """
    without_comments = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    return [s.strip() for s in without_comments.split(";") if s.strip()]


def _run_file(path: Path, **params) -> None:
    """Executa um arquivo SQL, statement a statement.

    Placeholders {chave} são interpolados via str.format — os valores vêm
    SEMPRE do registro validado (stages.config), nunca de entrada externa.
    """
    sql = path.read_text(encoding="utf-8")
    if params:
        sql = sql.format(**params)
    cursor = _connection().cursor()
    for statement in _statements(sql):
        cursor.execute(statement)
        cursor.fetchall()  # aguarda a conclusão do statement


def lake_init() -> None:
    """DDL idempotente: schemas e tabelas silver/gold (CREATE IF NOT EXISTS)."""
    _run_file(SQL_DIR / "init" / "00_lake_ddl.sql")


def silver_for_tenant(tenant_id: str, since_iso: str) -> None:
    """Aplica os eventos CDC de UM tenant na silver (um MERGE por tabela).

    `since_iso` delimita a janela de eventos lida da bronze — quem decide a
    janela é a orquestração (E4); overlap é seguro pela idempotência do MERGE.
    """
    if tenant_id not in config.tenant_ids():
        raise ValueError(f"tenant desconhecido: {tenant_id!r}")
    for table in config.table_names():
        _run_file(SQL_DIR / "silver" / f"{table}.sql", tenant_id=tenant_id, since=since_iso)
        print(f"silver: {table} atualizada para {tenant_id}")


def build_gold() -> None:
    """Reconstrói os marts da gold para TODOS os tenants (o particionamento
    por tenant_id no Iceberg mantém os dados fisicamente segregados)."""
    for path in sorted((SQL_DIR / "gold").glob("*.sql")):
        _run_file(path)
        print(f"gold: {path.stem} reconstruído")
