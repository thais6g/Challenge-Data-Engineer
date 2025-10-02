from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from delta import configure_spark_with_delta_pip
import os
import shutil

# --- CONFIGURAÇÕES DE DEPENDÊNCIA ---
# A dependência Delta Lake será resolvida pelo CMD do Docker,
# mas se rodarmos o arquivo isoladamente, precisamos desta configuração.
DELTA_PACKAGES = "io.delta:delta-core_2.12:3.1.0"
DELTA_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

# Inicializa Spark com suporte a Delta Lake
builder = SparkSession.builder \
    .appName("SilverTransformer") \
    .config("spark.jars.packages", DELTA_PACKAGES) \
    .config("spark.sql.extensions", DELTA_EXTENSIONS) \
    .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    #.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def transform_silver(origem="data/bronze", destino="data/silver"):
    """
    Leitura das tabelas presentes na camada Raw. 
    Definição de schema, rename de coluna e atualização de tipo de dado.
    Carga na camada Silver
    """
    os.makedirs(destino, exist_ok=True)
    print("\n--- Iniciando transformação para camada Silver ---")


    try:
        #Criação das tabelas na camada silver respeitando o schema solicitado
        df_empresas = spark.read.format("delta").load(os.path.join(origem, "Empresas"))
        df_empresas.select(
            col('CNPJ_BASICO').alias('cnpj'),
            col('RAZAO_SOCIAL_NOME_EMPRESARIAL').alias('razao_social'),
            col('NATUREZA_JURIDICA').cast('int').alias('natureza_juridica'),
            col('QUALIFICACAO_DO_RESPONSAVEL').cast('int').alias('qualificacao_responsavel'),
            regexp_replace('CAPITAL_SOCIAL_DA_EMPRESA', ',', '.').cast('double').alias('capital_social'),
            col('PORTE_DA_EMPRESA').alias('cod_porte')
        ).write.format("delta").mode("overwrite").save(os.path.join(destino, "Empresas"))
        print("Tabela EMPRESAS salva como Delta na camada Silver.")
    except Exception as e:
        print(f"Erro ao transformar EMPRESAS: {e}")

    try:
        df_socios = spark.read.format("delta").load(os.path.join(origem, "Socios"))
        df_socios.select(
            col('CNPJ_BASICO').alias('cnpj'),
            col('IDENTIFICADOR_DE_SOCIO').alias('tipo_socio'),
            col('NOME_DO_SOCIO_OU_RAZAO_SOCIAL').alias('nome_socio'),
            col('CNPJ_CPF_DO_SOCIO').alias('documento_socio'),
            col('QUALIFICACAO_DO_SOCIO').alias('codigo_qualificacao_socio')
        ).write.format("delta").mode("overwrite").save(os.path.join(destino, "Socios"))
        print("Tabela SOCIOS salva como Delta na camada silver.")
    except Exception as e:
        print(f"Erro ao transformar SOCIOS: {e}")

if __name__ == "__main__":
    transform_silver()