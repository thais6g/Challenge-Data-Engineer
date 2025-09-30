# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# --- CONFIGURAÇÕES DO SPARK ---
DELTA_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

spark = None

def get_spark_session():
    """Inicializa ou retorna a SparkSession configurada."""
    global spark
    if spark is None:
        builder = SparkSession.builder \
            .appName("PostgresExporter") \
            .config("spark.sql.extensions", DELTA_EXTENSIONS) \
            .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    return spark

def load_postgres(input_path="data/gold/final_result", table_name="final_result"):
    """
    Lê a tabela final da Camada Gold (Delta) e a carrega no PostgreSQL via JDBC.
    """
    spark = get_spark_session()
    print("\n--- Iniciando exportação para PostgreSQL ---")

    # 1. Configurações de Conexão (Lidas do docker-compose.yml)
    POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
    POSTGRES_USER = os.environ.get("POSTGRES_USER", "admin")
    POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "admin123")
    POSTGRES_DB = os.environ.get("POSTGRES_DB", "recfederal_db")
    
    # URL JDBC (usa o nome do serviço do Docker Compose como host)
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"
    driver = "org.postgresql.Driver"

    try:
        # 2. Leitura da Tabela Gold
        df_gold = spark.read.format("delta").load(input_path)
        print(f"Lidas {df_gold.count()} linhas da Camada Gold.")

        # 3. Escrita no PostgreSQL
        df_gold.write \
             .format("jdbc") \
             .option("url", jdbc_url) \
             .option("dbtable", table_name) \
             .option("user", POSTGRES_USER) \
             .option("password", POSTGRES_PASSWORD) \
             .option("driver", driver) \
             .mode("overwrite") \
             .save()

        print(f"✅ Tabela '{table_name}' carregada com sucesso no PostgreSQL!")

    except AnalysisException as e:
        print(f"🛑 ERRO DE ANÁLISE: Falha ao ler tabela Delta Gold. Erro: {e}")
    except Exception as e:
        print(f"🛑 ERRO NA EXPORTAÇÃO (PostgreSQL): Falha ao conectar ou escrever. Erro: {e}")

if __name__ == "__main__":
     load_postgres()
    


