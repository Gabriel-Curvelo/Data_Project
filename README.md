# Projeto de Engenharia de Dados

Este repositório apresenta uma pipeline de dados construída com foco em orquestração, armazenamento em data lake e processamento analítico com PySpark. A solução segue o padrão **Medallion Architecture**, com separação em camadas Bronze, Silver e Gold, utilizando um ambiente local com Docker para facilitar desenvolvimento, testes e demonstração.

A arquitetura combina **Astronomer Astro + Apache Airflow** para orquestração, **MinIO** como data lake compatível com S3, **Hive Metastore** para catálogo de tabelas, **Trino** para consultas SQL distribuídas sobre os dados no lake, **DBeaver** como client SQL para exploração analítica e **Great Expectations** para validações de qualidade de dados e geração de relatórios de conformidade.

## Arquitetura

A pipeline foi organizada em três camadas principais:

- **Bronze (`bronzelayer`)**: armazena os dados brutos ingeridos da fonte original, preservando ao máximo a estrutura de origem.
- **Silver (`silverlayer`)**: contém dados tratados, padronizados, tipados e preparados para consumo analítico intermediário.
- **Gold (`goldlayer`)**: reúne tabelas agregadas e analíticas, prontas para exploração em BI, SQL e relatórios.

### Fluxo da solução

1. O Airflow orquestra a execução das tasks de ingestão, transformação, validação e publicação.
2. Os dados brutos são gravados no MinIO na camada Bronze.
3. As transformações em PySpark geram a camada Silver com dados tratados e padronizados.
4. O Great Expectations executa validações de domínio, completude, faixa de valores, unicidade e conformidade das colunas críticas, além de registrar o resultado em relatório JSON na camada Gold.
5. As agregações analíticas geram as tabelas da camada Gold.
6. O Hive Metastore registra as tabelas externas apontando para os diretórios no MinIO.
7. O Trino consulta essas tabelas via catálogo Hive.
8. O DBeaver pode ser usado para explorar os dados em SQL.

## Tecnologias utilizadas

O projeto foi desenvolvido com ferramentas open source amplamente adotadas em engenharia de dados:

| Ferramenta | Finalidade |
|---|---|
| Docker | Subir e isolar todos os serviços localmente |
| Astronomer Astro | Facilitar o ambiente local do Apache Airflow |
| Apache Airflow | Orquestração da pipeline e agendamento das tasks |
| MinIO | Armazenamento das camadas Bronze, Silver e Gold |
| Apache Hive Metastore | Catálogo de metadados das tabelas externas |
| Trino | Engine SQL para consulta sobre o data lake |
| DBeaver | Cliente SQL para inspeção das tabelas e validação dos dados |
| Python + PySpark | Ingestão, transformação, limpeza e geração das camadas |
| Great Expectations | Data quality, regras de domínio, validações estruturais e relatório de conformidade 

## Estrutura Medallion

### Bronze

A camada Bronze armazena os dados exatamente como chegam da origem, servindo como zona de persistência inicial, auditoria e reprocessamento.

### Silver

A camada Silver contém os dados higienizados e padronizados, com tipagem consistente, normalização de colunas e estrutura pronta para análises mais confiáveis.

### Gold

A camada Gold concentra visões analíticas, agregações e relatórios de negócio prontos para consultas em SQL, BI e auditoria de qualidade.

## Como executar o projeto

### 1. Pré-requisitos

Antes de começar, instale:

- Docker
- Python 3.9 ou superior
- DBeaver
- Astro CLI

No Windows, via PowerShell, o Astro pode ser instalado com:

```powershell
winget install -e --id Astronomer.Astro
```

### 2. Clone o repositório

```bash
git clone https://github.com/Gabriel-Curvelo/Data_Project.git
cd Data_Project
```

### 3. Suba o ambiente do Airflow

Entre na pasta do Airflow e execute:

```bash
cd Airflow
astro dev start
```

Esse comando sobe o ambiente local do Airflow via Astronomer, incluindo os serviços necessários para desenvolvimento e execução das DAGs.

**Acesso ao Airflow**

- URL: [http://localhost:8080](http://localhost:8080)
- Usuário: `admin`
- Senha: `admin`

As credenciais acima são usadas apenas para desenvolvimento local.

### 4. Suba o Data Lake e os serviços de consulta

Em outro terminal, vá para a pasta `DataLake` e execute:

```bash
cd DataLake
docker-compose up -d
```

Esse comando sobe os serviços de armazenamento e consulta, incluindo:

- MinIO
- Hive Metastore
- PostgreSQL do Metastore, se configurado no `docker-compose`
- Trino

**Acesso ao MinIO**

- URL: [http://localhost:9001](http://localhost:9001)
- Usuário: `datalake`
- Senha: `datalake`

### 5. Crie os buckets do Data Lake

No painel do MinIO, crie os buckets:

- `bronzelayer`
- `silverlayer`
- `goldlayer`

### 6. Execute a DAG

No Airflow, execute a DAG responsável pela pipeline.

Ao final da execução, os dados deverão estar distribuídos nas camadas Bronze, Silver e Gold no MinIO.

## Catálogo e consultas SQL

Após a carga dos dados, as tabelas externas podem ser registradas no Hive Metastore e consultadas pelo Trino.

Na pasta `DataLake`, execute:

```powershell
Get-Content .\sql\register_medallion_tables.sql -Raw `
| docker exec -i trino trino
```

Para validar:

```sql
SHOW SCHEMAS FROM hive;
SHOW TABLES FROM hive.gold;
```

## Conexão com o DBeaver

Dentro do DBeaver, crie uma nova conexão com o Trino:

- Host: `localhost`
- Port: `8081`
- Database/Schema: deixe em branco
- Username: `admin`

Para testar:

```sql
SELECT *
FROM hive.gold.fraud_credit
LIMIT 50;
```

## Qualidade e monitoramento

O projeto implementa uma camada de **data quality** com Great Expectations integrada ao fluxo da pipeline. As validações são executadas sobre a camada Silver e geram um relatório estruturado em TXT na camada Gold, permitindo auditoria, rastreabilidade e acompanhamento das inconsistências encontradas.

As validações contemplam, entre outros pontos:

- Existência das colunas obrigatórias.
- Verificação de valores nulos em campos críticos.
- Validação de domínio para colunas categóricas, como `transaction_type`, `anomaly`, `age_group`, `purchase_pattern` e `location_region`.
- Validação de intervalos numéricos, como `risk_score`, `amount`, `login_frequency` e `session_duration`.
- Validação de padrões com regex para endereços e IPs.
- Verificação de unicidade composta para evitar duplicidades transacionais.
- Regras adicionais com PySpark para mapear inconsistências condicionais e registrar ocorrências no relatório final.

### Comportamento da camada de qualidade

A estratégia adotada para este projeto é **reportar** as inconsistências sem interromper a continuidade da DAG. Dessa forma, o relatório final registra falhas de conformidade, valores inesperados, distribuição de anomalias e outras métricas de qualidade, mas a pipeline pode seguir para fins de análise e demonstração.

Essa abordagem é útil em ambientes de estudo, demonstração técnica e exploração de dados, onde é importante evidenciar os problemas encontrados sem bloquear a geração da camada Gold.

## Credenciais e segurança

As credenciais presentes no ambiente local existem apenas para facilitar testes e desenvolvimento. Em ambientes produtivos, o ideal é utilizar gerenciamento seguro de segredos, segregação de acesso e variáveis protegidas.

## Organização esperada do repositório

```text
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
└── README.md
```

## Considerações finais

Este projeto demonstra uma arquitetura moderna de data lake local, unindo orquestração, armazenamento de objetos, catálogo de metadados, validação de qualidade e engine SQL em um fluxo reproduzível e didático. A combinação entre Airflow, MinIO, Hive, Trino, PySpark e Great Expectations permite simular localmente padrões muito próximos dos utilizados em ambientes reais de engenharia de dados.
