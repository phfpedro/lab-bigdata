#!/bin/bash
# E0 — Provisiona os bancos de origem: 1 database por tenant (o tenant_id É o
# nome do database, como no produto real). Lê o registro único de tenants.
set -euo pipefail

TENANTS=$(grep -E '^[[:space:]]*-[[:space:]]*id:' /config/tenants.yml | awk '{print $NF}')

i=1
for tenant in $TENANTS; do
    echo "── criando banco de origem do tenant: ${tenant}"
    createdb -U "$POSTGRES_USER" "$tenant"
    # seed e volume variam por tenant para os dados não serem idênticos
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$tenant" \
         -v seed="0.${i}" -v nproc=$((i * 400)) \
         -f /tenant-sql/tenant_schema.sql
    i=$((i + 1))
done

echo "── bancos de tenant prontos"
