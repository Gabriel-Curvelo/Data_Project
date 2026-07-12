Projeto de Engenharia de Dados
Este repositório apresenta uma pipeline de dados construída com foco em orquestração, armazenamento em data lake e consulta analítica sobre arquivos em formato colunar. A solução segue o padrão Medallion Architecture, com separação em camadas Bronze, Silver e Gold, e utiliza um stack local com Docker para facilitar desenvolvimento, testes e demonstração.

A arquitetura combina Astronomer/Astro + Apache Airflow para orquestração, MinIO como data lake compatível com S3, Hive Metastore para catálogo de tabelas, Trino para consulta SQL distribuída sobre os dados no lake, e DBeaver como client SQL para exploração das camadas analíticas.

Arquitetura
A pipeline foi organizada em três camadas principais:

Bronze (bronzelayer): armazena os dados brutos ingeridos da fonte original, preservando ao máximo a estrutura de origem.

Silver (silverlayer): contém dados tratados, padronizados e enriquecidos para consumo analítico intermediário.

Gold (goldlayer): reúne tabelas agregadas e analíticas, prontas para exploração em BI, SQL e relatórios.


Tecnologias utilizadas
O projeto foi desenvolvido com ferramentas open source amplamente adotadas em engenharia de dados:

Ferramentas:
Docker	Subir e isolar todos os serviços localmente
Astronomer Astro	Facilitar o ambiente local do Apache Airflow
Apache Airflow	Orquestração da pipeline e agendamento das tasks
MinIO	Armazenamento das camadas Bronze, Silver e Gold
Apache Hive Metastore	Catálogo de metadados das tabelas externas
Trino	Engine SQL para consulta sobre o data lake
DBeaver	Cliente SQL para inspeção das tabelas e validação dos dados
Python + PySpark	Ingestão, transformação e geração das camadas

Fluxo da pipeline
O fluxo esta sequência:

O Airflow orquestra a execução das tasks de ingestão, transformação e validação.

Os dados brutos são gravados no MinIO na camada Bronze.

As transformações em PySpark geram a camada Silver com dados tratados.

Novas agregações produzem a camada Gold com tabelas analíticas.

O Hive Metastore registra as tabelas externas apontando para os diretórios no MinIO.

O Trino consulta essas tabelas via catálogo Hive.

O DBeaver pode ser usado para explorar os dados em SQL.

Estrutura Medallion
Bronze
A camada Bronze armazena os dados exatamente como chegam da origem, servindo como zona de persistência inicial, reprocessamento e auditoria.

Silver
A camada Silver contém os dados higienizados e padronizados, com colunas derivadas e estrutura pronta para análises mais confiáveis.

Gold
A camada Gold concentra visões analíticas, agregações e tabelas de negócio prontas para consultas mais diretas em ferramentas SQL ou BI.

Como executar o projeto
1. Pré-requisitos
Antes de começar, instale:

Docker

Python 3.9 ou superior

DBeaver

Astro CLI

No Windows via PowerShell, o Astro pode ser instalado com:

powershell
winget install -e --id Astronomer.Astro
2. Clone o repositório
bash
git clone https://github.com/Gabriel-Curvelo/Data_Project.git
cd Data_Project
3. Suba o ambiente do Airflow
Entre na pasta do Airflow e execute:

bash
cd Airflow
astro dev start
Esse comando sobe o ambiente local do Airflow via Astronomer, incluindo os serviços necessários para desenvolvimento da DAG.

Acesso ao Airflow
URL: http://localhost:8080

Usuário: admin

Senha: admin

As credenciais acima são usadas apenas para desenvolvimento local.

4. Suba o Data Lake e os serviços de consulta
Em outro terminal, vá para a pasta DataLake e execute:

bash
cd DataLake
docker-compose up -d
Esse comando sobe os serviços de armazenamento e consulta, incluindo:

MinIO

Hive Metastore

PostgreSQL do Metastore (se configurado no compose)

Trino

Acesso ao MinIO
URL: http://localhost:9001

Usuário: datalake

Senha: datalake

5. Crie os buckets do data lake
No painel do MinIO, crie os buckets:

bronzelayer

silverlayer

goldlayer

6. Execute a DAG
No Airflow, execute a DAG responsável pela pipeline.

Ao final da execução, os dados deverão estar distribuídos nas camadas Bronze, Silver e Gold no MinIO.

Catálogo e consultas SQL
Após a carga dos dados, as tabelas externas podem ser registradas no Hive Metastore e consultadas pelo Trino.
.......







Qualidade e monitoramento
O projeto pode ser evoluído com práticas adicionais de observabilidade e confiança de dados, como:

Alertas automáticos em falhas de DAGs no Airflow

Validação de schema e integridade antes da publicação das camadas

Logs persistidos para troubleshooting

Dashboards de qualidade de dados

Testes de disponibilidade de arquivos nas camadas Silver e Gold

Credenciais e segurança
As credenciais presentes no ambiente local existem apenas para facilitar testes e desenvolvimento.


Organização esperada do repositório
Uma organização típica deste projeto pode seguir a ideia abaixo:

text
Data_Project/
├── Airflow/
│   ├── dags/
│   ├── include/
│   │   ├── scripts/
│   │   └── ...
│   └── ...
├── DataLake/
│   ├── hive/
│   ├── trino/
│   ├── sql/
│   ├── docker-compose.yml
│   └── ...
└── README.txt


Considerações finais
Este projeto demonstra uma arquitetura moderna de data lake local, unindo orquestração, armazenamento de objetos, catálogo de metadados e engine SQL em um fluxo reproduzível e didático. A combinação entre Airflow, MinIO, Hive e Trino permite simular localmente padrões muito próximos dos usados em ambientes reais de engenharia de dados.
