# Dicionário de termos — PoC multi-tenant

Um termo, um significado simples. Feito para quem **não** trabalha com dados
no dia a dia. Sempre que possível, o exemplo se refere a algo que já existe
neste projeto — veja também [dag-tasks.md](dag-tasks.md) (o passo a passo da
DAG) e [arquitetura.md](arquitetura.md) (a arquitetura completa).

Ordem alfabética. Use Ctrl+F para achar o termo.

---

**Airflow (Apache Airflow)**
O "gerente de tarefas" da nossa plataforma. Ele não processa dado nenhum —
só decide **quando** cada etapa roda e **em que ordem**, e avisa se algo
falhou. É como o maestro de uma orquestra: não toca nenhum instrumento, mas
garante que todos entrem na hora certa.

**Broker (Kafka broker)**
Um "servidor" do Kafka — o processo que de fato guarda e entrega as
mensagens. Não confundir com **tópico** (que é a categoria das mensagens):
o broker é a "agência dos correios"; o tópico é a "gaveta com etiqueta"
dentro dela. Um cluster de Kafka pode ter vários brokers trabalhando
juntos; no nosso laboratório, temos só 1.

**Bronze (camada bronze)**
A primeira parada dos dados dentro do lake: uma cópia **crua**, sem
tratamento, de tudo que aconteceu na origem. É o rascunho — feio, mas
fiel ao que aconteceu. *(No nosso pipeline: `bronze.cdc_events`.)*

**CDC (Change Data Capture)**
A técnica de "vigiar" um banco de dados e capturar **cada mudança** assim
que ela acontece — um INSERT, um UPDATE, um DELETE — sem precisar ficar
perguntando "o que mudou?" de tempos em tempos. É como ter uma câmera
apontada para o banco, gravando tudo em tempo real.

**Conector (connector)**
O programinha que faz a ponte entre um banco de origem e o Kafka — lê as
mudanças do banco e publica lá. Temos **um conector por tenant**. *(No
nosso pipeline: `cdc-tenant_acme`, `cdc-tenant_beta`, etc.)*

**Consumer (consumidor)**
Um programa que **lê** mensagens do Kafka. A nossa task `land_events` é um
consumer: ela lê os eventos publicados pelos conectores.

**Consumer group (grupo de consumidores)**
Um "nome de time" que agrupa consumers e faz o Kafka lembrar até onde
aquele time já leu. Se o time parar e voltar depois, ele continua de onde
parou — não relê tudo do zero. *(No nosso pipeline: o grupo `lake-landing`.)*

**DAG (Directed Acyclic Graph)**
O "roteiro" que o Airflow segue: uma lista de tarefas com setas indicando a
ordem ("isso só roda depois daquilo"). Sem loops — uma tarefa nunca depende
dela mesma. *(No nosso pipeline: `bpms_analytics`, ver
[dag-tasks.md](dag-tasks.md).)*

**Debezium**
O software específico que a gente usa para fazer CDC em bancos Postgres.
Ele é o "operador da câmera" que vigia o banco e manda cada mudança para o
Kafka.

**Envelope (envelope Debezium)**
O "pacote" que o Debezium monta para cada mudança capturada: contém o
estado antigo da linha, o estado novo, e o tipo de operação
(insert/update/delete). Formato: JSON. Exemplo simplificado:
`{"before": {...}, "after": {...}, "op": "u"}`.

**Foto atual (estado atual)**
Jeito simples de descrever a **silver**: em vez de guardar "aconteceu isso,
depois aquilo", ela guarda só "como está agora, depois de todas as
mudanças aplicadas". Se um processo foi editado 5 vezes, a silver só mostra
a versão mais recente.

**Gold (camada gold)**
A última camada do lake: métricas e agregados **prontos** para virar
gráfico ou dashboard, sem precisar de mais cálculo. Também chamada de
**mart** (ver abaixo). *(No nosso pipeline: `gold.process_summary_by_type`,
`gold.daily_activity`.)*

**Iceberg (Apache Iceberg)**
Um "formato de tabela" que a gente usa para guardar os dados no lake. A
diferença dele para um arquivo comum é que ele permite **atualizar e
apagar** linhas específicas (com o `MERGE`) e guarda um histórico de
versões — coisas que um arquivo simples não sabe fazer sozinho.

**Kafka (Apache Kafka)**
Uma "fila de mensagens" gigante e organizada. Os conectores publicam
eventos nela, e outros programas leem esses eventos — sem que
publicador e leitor precisem se conhecer diretamente. Funciona como uma
central de correios: quem envia não precisa saber quem vai buscar.

**Lag**
Quantas mensagens estão **esperando para ser lidas** no Kafka por um
consumer group. Lag alto = tem gente atrasado lendo a fila. Lag zero =
tudo lido, nada pendente.

**Lake (data lake)**
O "depósito" central onde todos os dados brutos e processados ficam
guardados, organizados em camadas (bronze/silver/gold). No nosso caso, o
depósito físico é o MinIO (que imita o Amazon S3).

**MERGE**
O comando SQL que aplica mudanças numa tabela de forma inteligente: se a
linha já existe, atualiza; se não existe, insere; se veio uma exclusão,
apaga. É o comando que transforma os eventos crus da bronze na "foto
atual" da silver.

**MinIO**
Um programa que se comporta exatamente como o armazenamento em nuvem da
Amazon (S3), mas roda na sua própria máquina. Usamos ele no laboratório
para simular o S3 sem precisar de conta na AWS.

**Offset**
Uma "marcação de página" dentro de uma fila do Kafka: indica até onde um
consumer já leu. Se o offset está em 400, significa "já li as primeiras
400 mensagens, a próxima é a 401".

**Orquestração**
O ato de coordenar várias tarefas para rodarem na ordem certa, na hora
certa, com direito a repetir se der erro. É o "trabalho" do Airflow.

**Partição (partição no lake)**
Uma forma de organizar os dados em "gavetas" separadas por algum critério
— no nosso caso, por `tenant_id`. Isso deixa a leitura mais rápida
(só abre a gaveta do tenant que interessa) e ajuda a manter isolamento
físico. *Atenção: não confundir com "partição do Kafka" — são conceitos
com o mesmo nome, mas contextos diferentes.*

**Partição (partição do Kafka)**
Uma "faixa" de uma fila do Kafka que permite processar mensagens em
paralelo. No nosso laboratório, cada tópico tem só 1 partição (é pequeno);
em produção, tópicos grandes teriam várias.

**Publicar / Publish**
Nesse projeto, tem dois sentidos parecidos: (1) o conector Debezium
**publica** eventos no Kafka; (2) a task `publish_tenant` **publica** as
métricas prontas (gold) no banco exclusivo do tenant (serving). No segundo
caso, é o mesmo que "entregar a versão final para consumo".

**Schema**
Palavra usada com dois sentidos diferentes, dependendo do contexto:
(1) a **estrutura** de uma tabela (quais colunas ela tem e de que tipo);
(2) uma "pasta" dentro de um banco de dados que agrupa tabelas (ex.:
`bronze`, `silver`, `gold` são schemas dentro do catálogo Iceberg).

**Serving (camada de serving / serving layer)**
A "vitrine" final dos dados: um banco de dados rápido e simples, já com
tudo pronto, para a aplicação ou o dashboard consultar sem esforço. É a
última parada antes do dado virar tela para o usuário. *(No nosso
pipeline: os bancos `serving_tenant_acme`, `serving_tenant_beta`, etc.)*

**Silver (camada silver)**
A segunda camada do lake: os dados da bronze já **limpos e organizados**,
representando o estado atual de cada tabela (ver "foto atual"). *(No
nosso pipeline: `silver.processes`, `silver.protocols`,
`silver.process_types`.)*

**Task**
Uma tarefa dentro de uma DAG do Airflow — um passo específico do trabalho.
Ex.: `land_events`, `build_gold`. Cada task faz uma coisa só, e o Airflow
garante a ordem entre elas.

**Task mapeada (dynamic task mapping)**
Quando uma task se **multiplica automaticamente**, uma cópia para cada
item de uma lista — no nosso caso, uma cópia por tenant. Se um tenant novo
aparece no registro, uma cópia nova da task aparece sozinha, sem precisar
mexer em código.

**Tópico (topic)**
Um "canal" dentro do Kafka, dedicado a um tipo de mensagem. Cada
combinação de tenant + tabela tem o seu: ex. `tenant_acme.public.processes`
guarda só as mudanças da tabela `processes` do tenant_acme.

**Trino**
O "motor de cálculo" que executa os comandos SQL sobre os dados do lake —
é ele quem roda os `MERGE` (bronze → silver) e os cálculos de agregação
(silver → gold).

**Watermark**
Uma técnica de dizer "só me traga o que mudou depois desse ponto no
tempo" (ex.: "depois das 14h de ontem"). É uma alternativa mais simples ao
CDC, mas que não enxerga updates/deletes de linhas antigas — por isso não
usamos ela sozinha neste projeto.

---

## Siglas rápidas

| Sigla | Significado |
| --- | --- |
| CDC | Change Data Capture |
| DAG | Directed Acyclic Graph |
| SQL | Structured Query Language |
| S3 | Simple Storage Service (armazenamento da Amazon) |
