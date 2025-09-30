challenge_data_engineer/
├── main.py                     # Script principal que orquestra o pipeline
├── src/                        # Módulos de processamento
│   ├── __init__.py             # Torna 'src' um pacote Python
│   ├── ingestion_receitafederal.py  # Baixa e extrai os arquivos da Receita Federal
│   ├── raw_loader.py           # Lê os arquivos extraídos e salva como Delta (camada Raw)
│   ├── silver_transformer.py   # Aplica transformações e salva como Delta (camada Silver)
│   ├── gold_builder.py         # Une tabelas e gera campos derivados (camada Gold)
│   ├── database.py             # Carrega a tabela Gold no banco PostgreSQL
├── data/                       # Diretório de dados
│   ├── raw/
│   │   ├── zip/                # Arquivos ZIP originais
│   │   └── extraction/         # Arquivos CSV extraídos
│   ├── silver/                 # Dados transformados (Silver)
│   └── gold/                   # Dados finais (Gold)
├── jars/                       # Driver JDBC do PostgreSQL (.jar)
│   └── postgresql-42.7.1.jar
├── requirements.txt            # Dependências Python
├── Dockerfile                  # Define o ambiente do Spark
└── docker-compose.yml          # Orquestra os serviços Spark e PostgreSQL

⚙️ Etapas do Pipeline
1. ingestion_receitafederal.py
- Realiza o download dos arquivos da Receita Federal.
- Extrai os arquivos ZIP para data/raw/extraction.
2. raw_loader.py
- Lê os arquivos CSV extraídos.
- Aplica os esquemas definidos para SOCIOS e EMPRESAS.
- Salva os dados como tabelas Delta em data/raw.
3. silver_transformer.py
- Lê os dados da camada Raw.
- Aplica transformações e renomeia colunas.
- Salva os dados como tabelas Delta em data/silver.
4. gold_builder.py
- Une as tabelas EMPRESAS e SOCIOS.
- Cria os campos derivados:
- qtde_socios
- flag_socio_estrangeiro
- doc_alvo
- Salva a tabela final em data/gold.
5. database.py
- Lê a tabela Gold.
- Conecta ao PostgreSQL via JDBC.
- Salva os dados na tabela empresas_socios_gold.

🚀 Como Executar
1. Instale o driver JDBC
- Baixe o arquivo .jar do site jdbc.postgresql.org
- Coloque em jars/postgresql-42.7.1.jar
2. Construa e execute com Docker Compose
docker-compose up --build


3. Acompanhe os logs
- O pipeline será executado automaticamente via main.py
- A tabela final será carregada no banco desafio_db no PostgreSQL

🧪 Teste e Visualização
- Acesse o PostgreSQL via pgAdmin ou DBeaver:
- Host: localhost
- Porta: 5432
- Usuário: admin
- Senha: admin123
- Banco: desafio_db

📌 Requisitos
- Docker e Docker Compose instalados
- Python 3.9+
- Spark com suporte a Delta Lake
- PostgreSQL JDBC Driver (.jar)
