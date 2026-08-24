"""E2 — Lake / landing: aterrissa os eventos CDC na bronze.

Contrato de ENTRADA (vem da E1 — extração):
    Tópicos Kafka `<tenant_id>.public.<tabela>` contendo o envelope Debezium
    em JSON (schemas desabilitados):
        { "before": {...}|null, "after": {...}|null,
          "source": {"lsn": ..., "table": ...}, "op": "c|u|d|r", "ts_ms": ... }

Contrato de SAÍDA (consumido pela E3 — transformação):
    Tabela Iceberg `bronze.cdc_events`, append-only, particionada por
    tenant_id + dia de extração, com as colunas:
        tenant_id, source_table, op, ts_ms, lsn,
        key_json, before_json, after_json, extracted_at

Este módulo é agnóstico de schema de negócio DE PROPÓSITO: os payloads ficam
em JSON cru e só a silver (E3) conhece a tipagem das tabelas. Assim, tabela
nova na origem não exige mudança aqui — só o SQL da silver correspondente.

Semântica: at-least-once. Os offsets do consumer group só são confirmados
DEPOIS do append no Iceberg; eventos eventualmente relidos são deduplicados
pelo MERGE idempotente da silver (rn=1 por PK, ordenado por lsn/ts_ms).
"""

import json
import os
from datetime import datetime, timezone

import pyarrow as pa
from confluent_kafka import Consumer
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

from stages import config

BRONZE_TABLE = "bronze.cdc_events"

_SCHEMA = Schema(
    NestedField(1, "tenant_id", StringType()),
    NestedField(2, "source_table", StringType()),
    NestedField(3, "op", StringType()),
    NestedField(4, "ts_ms", LongType()),
    NestedField(5, "lsn", LongType()),
    NestedField(6, "key_json", StringType()),
    NestedField(7, "before_json", StringType()),
    NestedField(8, "after_json", StringType()),
    NestedField(9, "extracted_at", TimestamptzType()),
)

_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="tenant_id"),
    PartitionField(source_id=9, field_id=1001, transform=DayTransform(), name="extracted_day"),
)

_ARROW = pa.schema(
    [
        pa.field("tenant_id", pa.string()),
        pa.field("source_table", pa.string()),
        pa.field("op", pa.string()),
        pa.field("ts_ms", pa.int64()),
        pa.field("lsn", pa.int64()),
        pa.field("key_json", pa.string()),
        pa.field("before_json", pa.string()),
        pa.field("after_json", pa.string()),
        pa.field("extracted_at", pa.timestamp("us", tz="UTC")),
    ]
)


def _catalog():
    return load_catalog(
        "lake",
        **{
            "type": "rest",
            "uri": os.environ["ICEBERG_REST_URI"],
            "warehouse": os.environ.get("LAKE_WAREHOUSE", "s3://lake/"),
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
            "s3.region": os.environ.get("S3_REGION", "us-east-1"),
        },
    )


def ensure_bronze():
    """Cria namespace/tabela da bronze se não existirem (idempotente)."""
    catalog = _catalog()
    catalog.create_namespace_if_not_exists("bronze")
    return catalog.create_table_if_not_exists(BRONZE_TABLE, schema=_SCHEMA, partition_spec=_SPEC)


def _parse(msg) -> dict | None:
    """Envelope Debezium → linha da bronze. None para mensagens irrelevantes."""
    envelope = json.loads(msg.value())
    if not isinstance(envelope, dict) or "op" not in envelope:
        return None
    source = envelope.get("source") or {}
    return {
        "tenant_id": msg.topic().split(".")[0],  # topic.prefix = nome do banco
        "source_table": source.get("table") or msg.topic().rsplit(".", 1)[-1],
        "op": envelope["op"],
        "ts_ms": envelope.get("ts_ms"),
        "lsn": source.get("lsn"),
        "key_json": msg.key().decode() if msg.key() else None,
        "before_json": json.dumps(envelope["before"]) if envelope.get("before") is not None else None,
        "after_json": json.dumps(envelope["after"]) if envelope.get("after") is not None else None,
    }


def land(max_events: int = 100_000) -> int:
    """Drena os tópicos CDC em micro-batch e faz append na bronze.

    Para de consumir após 3 polls vazios (tópicos drenados) ou ao atingir
    max_events (limita memória da task; o excedente fica para o próximo run).
    """
    table = ensure_bronze()
    topics = [
        f"{tenant}.public.{tbl}"
        for tenant in config.tenant_ids()
        for tbl in config.table_names()
    ]
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
            "group.id": "lake-landing",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit manual: só após persistir
        }
    )
    consumer.subscribe(topics)

    rows: list[dict] = []
    polled = False
    empty_polls = 0
    extracted_at = datetime.now(timezone.utc)
    try:
        while empty_polls < 3 and len(rows) < max_events:
            msgs = consumer.consume(num_messages=500, timeout=5.0)
            if not msgs:
                empty_polls += 1
                continue
            empty_polls = 0
            for msg in msgs:
                if msg.error() or msg.value() is None:
                    continue
                polled = True
                row = _parse(msg)
                if row is not None:
                    row["extracted_at"] = extracted_at
                    rows.append(row)

        if rows:
            table.append(pa.Table.from_pylist(rows, schema=_ARROW))
        if polled:
            # offsets confirmados somente depois do append (at-least-once)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    print(f"landing: {len(rows)} eventos gravados em {BRONZE_TABLE}")
    return len(rows)
