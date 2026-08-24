#!/bin/bash
# ═══ E5 — Serving: provisionamento (1 DATABASE por tenant) ═══════════════════
# A parede de isolamento é ESTRUTURAL: uma conexão Postgres entra em um único
# database e não alcança os demais. Reforçamos revogando o CONNECT default do
# PUBLIC — sem isso, qualquer role logaria em qualquer database.
#
# Convenções (derivadas do registro único config/tenants.yml):
#   database serving_<tenant> | role svc_<tenant> | senha pw_<tenant>  (LAB!)
#
# Em produção: senhas em secrets manager e provisionamento automatizado no
# onboarding do tenant (mesma fonte de verdade do cadastro).
set -euo pipefail

TENANTS=$(grep -E '^[[:space:]]*-[[:space:]]*id:' /config/tenants.yml | awk '{print $NF}')

for tenant in $TENANTS; do
    role="svc_${tenant}"
    db="serving_${tenant}"
    echo "── provisionando serving do tenant: ${tenant}"
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres <<SQL
CREATE ROLE ${role} LOGIN PASSWORD 'pw_${tenant}';
CREATE DATABASE ${db};
REVOKE CONNECT ON DATABASE ${db} FROM PUBLIC;
GRANT CONNECT ON DATABASE ${db} TO ${role};
SQL
    # a role consulta o schema public do próprio database (SELECT é concedido
    # pelo publish, tabela a tabela, após criá-las)
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$db" \
         -c "GRANT USAGE ON SCHEMA public TO ${role};"
done

echo "── servings provisionados"
