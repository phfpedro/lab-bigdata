"""Registro único de tenants e tabelas.

Contrato: toda etapa descobre QUAIS tenants e tabelas existem por aqui —
nunca por listas próprias. Adicionar tenant/tabela = editar config/*.yml
(a mesma fonte que provisiona bancos de origem, conectores CDC e serving).

Os ids são validados contra um padrão estrito porque viram nome de database,
tópico Kafka, role e interpolação em SQL — a validação é a primeira linha
de defesa contra injeção via configuração.
"""

import os
import re

import yaml

CONFIG_DIR = os.environ.get("CONFIG_DIR", "/opt/airflow/config")

# minúsculas/dígitos/underscore, começando por letra — seguro para db/topic/SQL
_SAFE_ID = re.compile(r"^[a-z][a-z0-9_]*$")


def _load(filename: str) -> dict:
    with open(os.path.join(CONFIG_DIR, filename), encoding="utf-8") as f:
        return yaml.safe_load(f)


def _validated(values: list[str], kind: str) -> list[str]:
    for value in values:
        if not _SAFE_ID.fullmatch(value):
            raise ValueError(f"{kind} inválido no registro: {value!r}")
    return values


def tenant_ids() -> list[str]:
    """Tenants ativos, na ordem do registro."""
    return _validated([t["id"] for t in _load("tenants.yml")["tenants"]], "tenant_id")


def table_names() -> list[str]:
    """Tabelas de negócio capturadas por CDC."""
    return _validated([t["name"] for t in _load("tables.yml")["tables"]], "table")
