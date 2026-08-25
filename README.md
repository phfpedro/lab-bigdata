# Plataforma analítica multi-tenant (BPMS)

Pipeline de dados para um serviço multi-tenant onde **cada tenant tem seu próprio banco de origem**. O fluxo captura mudanças dos bancos de origem em tempo quase real (CDC), organiza tudo em camadas num data lake (Iceberg), transforma com SQL (Trino), e publica o resultado num banco de **serving exclusivo por tenant** — com prova automatizada de que nenhum tenant consegue acessar o dado de outro. Um dashboard (Metabase) no final demonstra o valor do pipeline visualmente.

> 📖 Quer entender o "porquê" de cada escolha (contratos entre etapas, decisões, mapeamento pra produção)? Ver [docs/arquitetura.md](docs/arquitetura.md).

## Passo a passo: rodar e testar, do zero até o dashboard

### 1. Pré-requisito

Docker e Docker Compose instalados, com pelo menos 6 GB de memória disponíveis para o Docker (Trino e Metabase são os mais pesados).

### 2. Suba o stack

```bash
docker compose up -d --build
```

Aguarde ~2 min (Trino e Airflow demoram mais a ficar prontos).

### 3. Dispare o pipeline

Abra o Airflow em <http://localhost:8080> → DAG `bpms_analytics` → botão **▶ Trigger DAG**. Acompanhe até todas as tasks ficarem verdes (o 1º ciclo consome o snapshot inicial e materializa tudo até o serving). Ela também roda sozinha a cada 5 min.

### 4. Conecte o Metabase (<http://localhost:3000>, só na 1ª vez)

1. Crie a conta de admin.
2. Conecte um banco **PostgreSQL**, pelo campo **"Connection string"**:

   ```text
   jdbc:postgresql://serving-db:5432/serving_tenant_acme?user=svc_tenant_acme&password=pw_tenant_acme
   ```

   Ou campo a campo: Host `serving-db` · Porta `5432` · Database `serving_tenant_acme` · Usuário `svc_tenant_acme` · Senha `pw_tenant_acme`.

### 5. Crie os gráficos

1. **"+ New" → "Question"** → tabela `process_summary_by_type` → **Visualize** → barras (eixo X `process_type`, eixo Y `avg_duration_hours`) → **Save**
2. Repita para `daily_activity` (linha/barra por `activity_date`)

### 6. Monte a dashboard

**"+ New" → "Dashboard"** → **"Add a chart"** com as duas perguntas salvas → **Save**.

Para derrubar tudo (incluindo os dados): `docker compose down -v`

## Endpoints de referência

| Serviço | URL / acesso | Credenciais |
| --- | --- | --- |
| Airflow UI | <http://localhost:8080> | sem login (ambiente local) |
| MinIO Console | <http://localhost:9001> | minioadmin / minioadmin |
| Kafka Connect | <http://localhost:8083> | — |
| Bancos tenant | localhost:5433 | postgres / tenants |
| Serving | localhost:5434 | postgres / serving (admin) |
| Metabase (dashboard) | <http://localhost:3000> | criado no 1º acesso |

## Mais documentação

| Documento | Conteúdo |
| --- | --- |
| [docs/arquitetura.md](docs/arquitetura.md) | Contratos entre etapas, decisões e por quê, mapeamento pra produção, pendências conscientes |
| [docs/fluxo-completo.md](docs/fluxo-completo.md) | O fluxo inteiro em ordem cronológica, do `docker compose up` ao dashboard |
| [docs/dag-tasks.md](docs/dag-tasks.md) | O que cada task da DAG faz, com exemplos |
| [docs/glossario.md](docs/glossario.md) | Dicionário de termos em linguagem simples |
| [docs/stack.md](docs/stack.md) | Cada tecnologia: o que é e o que faz aqui |
| [docs/sistemas/](docs/sistemas/) | Aprofundamento por tecnologia: por quê, conceitos-chave, como explorar |
