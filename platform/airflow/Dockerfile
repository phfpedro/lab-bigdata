FROM apache/airflow:3.0.2

# Dependências por etapa (cada uma substituível sem afetar as demais):
#   confluent-kafka → E2 landing: consome os tópicos CDC
#   pyiceberg       → E2 landing: escreve na bronze (REST catalog + S3/MinIO)
#   trino           → E3 transformação: MERGE silver / agregações gold
# (psycopg2 e PyYAML já vêm na imagem base — usados por E5 e pela config)
RUN pip install --no-cache-dir \
    "confluent-kafka>=2.5" \
    "pyiceberg[pyarrow,s3fs]>=0.9,<1" \
    "trino>=0.330"
