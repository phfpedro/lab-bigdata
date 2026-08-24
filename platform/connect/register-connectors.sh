#!/bin/sh
# ═══ E1 — Extração (CDC) ═════════════════════════════════════════════════════
# Registra no Kafka Connect um conector Debezium POR TENANT, lendo o registro
# único (config/tenants.yml) e as tabelas capturadas (config/tables.yml).
#
# Contrato de saída da E1: tópicos Kafka `<tenant_id>.public.<tabela>` com o
# envelope Debezium em JSON (schemas desabilitados):
#   { "before": {...}|null, "after": {...}|null,
#     "source": { "lsn": ..., "table": ..., ... }, "op": "c|u|d|r", "ts_ms": ... }
#
# O tenant_id nasce aqui: é o nome do database, virando o prefixo do tópico.
# Atenção: slot de replicação é único por INSTÂNCIA postgres → slot.name leva
# o tenant no nome para não colidir entre os conectores.
set -e

echo "aguardando o Kafka Connect subir..."
until curl -sf http://connect:8083/connectors >/dev/null; do sleep 3; done

TENANTS=$(grep -E '^[[:space:]]*-[[:space:]]*id:' /config/tenants.yml | awk '{print $NF}')
TABLES=$(grep -E '^[[:space:]]*-[[:space:]]*name:' /config/tables.yml \
         | awk '{print "public."$NF}' | tr '\n' ',' | sed 's/,$//')

for t in $TENANTS; do
  echo "registrando conector cdc-${t} (tabelas: ${TABLES})"
  curl -sf -X PUT -H 'Content-Type: application/json' \
    "http://connect:8083/connectors/cdc-${t}/config" -d @- <<EOF
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "tenant-db",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "tenants",
  "database.dbname": "${t}",
  "topic.prefix": "${t}",
  "slot.name": "cdc_${t}",
  "publication.name": "dbz_${t}",
  "plugin.name": "pgoutput",
  "snapshot.mode": "initial",
  "table.include.list": "${TABLES}",
  "decimal.handling.mode": "double",
  "tombstones.on.delete": "false",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
EOF
  echo ""
done

echo "conectores CDC registrados."
