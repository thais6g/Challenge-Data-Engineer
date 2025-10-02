from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
import shutil


# --- CONFIGURAÇÕES DE DEPENDÊNCIA ---
# A dependência Delta Lake será resolvida pelo CMD do Docker,
# mas se rodarmos o arquivo isoladamente, precisamos desta configuração.
DELTA_PACKAGES = "io.delta:delta-core_2.12:3.1.0"
DELTA_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

# Variável global para armazenar a SparkSession após a criação
spark = None

# --- 1. DEFINIÇÃO DE SCHEMAS FIXOS (Todos StringType para a Camada bronze) ---
# Os nomes das colunas devem corresponder EXATAMENTE à ordem dos campos do arquivo ingerido e para isso foi consultado o metadado disponibilizado pela Receita Federal.
schema_socio = StructType([
    StructField("CNPJ_BASICO", StringType(), True),
    StructField("IDENTIFICADOR_DE_SOCIO", StringType(), True),
    StructField("NOME_DO_SOCIO_OU_RAZAO_SOCIAL", StringType(), True),
    StructField("CNPJ_CPF_DO_SOCIO", StringType(), True),
    StructField("QUALIFICACAO_DO_SOCIO", StringType(), True),
    StructField("DATA_DE_ENTRADA_SOCIEDADE", StringType(), True),
    StructField("PAIS", StringType(), True),
    StructField("REPRESENTANTE_LEGAL", StringType(), True),
    StructField("NOME_DO_REPRESENTANTE", StringType(), True),
    StructField("QUALIFICACAO_REPRESENTANTE_LEGAL", StringType(), True),
    StructField("FAIXA_ETARIA", StringType(), True)
])

schema_empresa = StructType([
    StructField("CNPJ_BASICO", StringType(), True),
    StructField("RAZAO_SOCIAL_NOME_EMPRESARIAL", StringType(), True),
    StructField("NATUREZA_JURIDICA", StringType(), True),
    StructField("QUALIFICACAO_DO_RESPONSAVEL", StringType(), True),
    StructField("CAPITAL_SOCIAL_DA_EMPRESA", StringType(), True), 
    StructField("PORTE_DA_EMPRESA", StringType(), True),
    StructField("ENTE_FEDERATIVO_RESPONSAVEL", StringType(), True)
])


def get_spark_session():
    """Inicializa ou retorna a SparkSession configurada para Delta Lake."""
    global spark
    if spark is None:
        builder = SparkSession.builder \
            .appName("BronzeLoader") \
            .config("spark.jars.packages", DELTA_PACKAGES) \
            .config("spark.sql.extensions", DELTA_EXTENSIONS) \
            .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG) \
            .config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), 'data', 'spark-warehouse')) \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
            .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile={os.getcwd()}/log4j2.properties") \
            .config("spark.jars.ivy", "/tmp/.ivy2")
        

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR") #Redução dos logs em tela apenas para os logs de ERROR
               
        print("SparkSession inicializada com suporte ao Delta Lake.")
    return spark


def load_bronze(origem="data/landed/extraction", destino="data/bronze"):
    """
    Lê os arquivos CSV extraídos, camada landed, aplica o schema StringType e salva como tabela Delta na camada bronze.
    """
    spark = get_spark_session() # Inicializa o Spark AQUI!

    # Lista arquivos válidos na pasta de extração
    arquivos = [f for f in os.listdir(origem) if f.endswith(('.CSV', '.SOCIOCSV', '.EMPRECSV'))]
    
    if not arquivos:
        print(f"Aviso: Nenhum arquivo encontrado na origem {origem}. Verifique a Etapa 1.")
        return

    for arquivo in arquivos:
        caminho_completo = os.path.join(origem, arquivo)
        print(f"\nLendo o arquivo: {arquivo}")

        if 'SOCIO' in arquivo.upper():
            nome_tabela = 'Socios'
            schema_tabela = schema_socio
        elif 'EMPRE' in arquivo.upper():
            nome_tabela = 'Empresas'
            schema_tabela = schema_empresa
        else:
            print(f"Arquivo {arquivo} não mapeado. Pulando...")
            continue

        try:
            # Leitura com schema fixo (header=False porque os arquivos da RFB não têm cabeçalho)
            df = spark.read.csv(
                caminho_completo,
                sep=';',
                encoding='latin1',
                header=False,
                schema=schema_tabela
            )
        
            caminho_delta = os.path.join(destino, nome_tabela)

            #Limpeza do diretório
            if os.path.exists(caminho_delta):
                shutil.rmtree(caminho_delta)

            # Salva o DataFrame como tabela Delta
            df.write.format("delta").mode("overwrite").save(caminho_delta)
            print(f"Tabela '{nome_tabela}' salva em Delta em: {caminho_delta}")

        except Exception as e:
            print(f"ERRO ao processar {arquivo} para Delta: {e}")
            
# Se for executado diretamente, inicializa o Spark e tenta rodar (útil para testes)
if __name__ == "__main__":
    load_bronze()
