# MinIO — aprofundamento

> Visão geral rápida: [stack.md](../stack.md). Fluxo completo: [fluxo-completo.md](../fluxo-completo.md).

## O que é

Um servidor de **armazenamento de objetos** open-source que implementa a
mesma API do Amazon S3. "Objeto" aqui significa arquivo: você sobe (`PUT`) e
baixa (`GET`) blobs identificados por uma chave (path), sem estrutura de
pastas real por baixo — diferente de um sistema de arquivos tradicional ou
de um banco relacional. É a camada mais "burra" e barata da stack: não sabe
nada sobre linhas, colunas ou schemas — só guarda bytes.

## Por que MinIO neste projeto

Não é uma decisão de arquitetura por si (não aparece na tabela de decisões)
— é a peça que permite rodar o laboratório **inteiro localmente, sem conta
AWS**, simulando o S3 que seria usado em produção. Qualquer código escrito
contra a API do MinIO (como o Iceberg REST catalog e o Trino fazem aqui)
funciona sem alteração apontando pro S3 real — é só trocar endpoint e
credenciais.

## Conceitos-chave para estudar

- **API compatível com S3**: MinIO fala o mesmo protocolo HTTP/REST que o
  S3 da AWS (`PUT`, `GET`, `DELETE`, listagem por prefixo). Isso é o que
  torna a troca por S3 em produção transparente para tudo que fica em cima
  dele (Iceberg, Trino).
- **Bucket**: o "namespace" de mais alto nível — como uma pasta raiz. Este
  projeto cria um único bucket, `lake`, na inicialização.
- **Path-style vs. virtual-hosted-style**: duas formas de endereçar um
  objeto na API S3 (`endpoint/bucket/chave` vs. `bucket.endpoint/chave`).
  MinIO local usa path-style (`CATALOG_S3_PATH__STYLE__ACCESS: "true"` na
  config do catálogo Iceberg) porque não tem DNS coringa por trás do
  hostname `minio`.
- **`mc` (MinIO Client)**: a ferramenta de linha de comando usada pelo
  serviço `minio-init` para criar o bucket no boot (`mc mb`) — vale olhar
  os comandos em [docker-compose.yml](../../docker-compose.yml).
- **Não confundir com o formato dos arquivos**: o MinIO guarda os arquivos
  Parquet que compõem as tabelas Iceberg, mas não sabe que eles formam uma
  tabela — quem entende essa organização é o catálogo Iceberg e o Trino.
  Ver [sistemas/iceberg.md](iceberg.md).

## Onde ver isso rodando neste projeto

- [docker-compose.yml](../../docker-compose.yml) — serviços `minio` e `minio-init`.
- Console web: <http://localhost:9001> (usuário/senha: `minioadmin`/`minioadmin`).

## Para explorar na prática (lab rodando)

1. Abra <http://localhost:9001> e entre com `minioadmin` / `minioadmin`.
2. Navegue até o bucket `lake` → pastas `bronze/`, `silver/`, `gold/` — cada
   uma cheia de arquivos `.parquet` e metadados `.json`/`.avro` do Iceberg.
   Isso é literalmente o "banco de dados" do lake, feito de arquivos comuns.
3. Repare que cada mudança gerada por um `MERGE` cria **arquivos novos**, não
   edita os antigos — objetos em object storage são imutáveis; é o Iceberg
   que gerencia qual conjunto de arquivos representa a versão atual da
   tabela (ver [sistemas/iceberg.md](iceberg.md)).

## Caminho pra produção

Vira Amazon S3 (gerenciado) sem mudar nenhum código — só endpoint e
credenciais. Self-hosted, continua MinIO (é usado em produção por muita
empresa que não quer depender de nuvem pública).
