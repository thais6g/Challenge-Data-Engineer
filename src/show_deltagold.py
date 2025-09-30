from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def show_deltagold():
    """Valida a leitura das tabelas Delta na camada GOLD e exibe visões filtradas."""

    # --- CONFIGURAÇÕES DE DEPENDÊNCIA ---
    DELTA_PACKAGES = "io.delta:delta-core_2.12:3.1.0"
    DELTA_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
    DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    # Inicializa Spark com suporte ao Delta Lake
    spark = SparkSession.builder \
        .appName("ValidationDeltaGold") \
        .config("spark.jars.packages", DELTA_PACKAGES) \
        .config("spark.sql.extensions", DELTA_EXTENSIONS) \
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    base_path = "data/gold/final_result"

    print(f"\n🔍 Lendo tabela Delta: final_result")
    try:
        df = spark.read.format("delta").load(base_path)
        print(f"✅ Tabela 'final_result' lida com sucesso. Total de registros: {df.count()}")
        df.printSchema()
        df.show(5, truncate=False)

        # 👥 Visão 1: Empresas com pelo menos um sócio
        print("\n👥 Visão: Empresas com pelo menos um sócio")
        df_com_socios = df.filter(col("qtde_socios") > 0)
        print(f"Total com sócios: {df_com_socios.count()}")
        df_com_socios.show(5, truncate=False)

        # 🌍 Visão 2: Empresas com sócio estrangeiro
        print("\n🌍 Visão: Empresas com sócio estrangeiro")
        df_estrangeiro = df.filter(col("flag_socio_estrangeiro") == True)
        print(f"Total com sócio estrangeiro: {df_estrangeiro.count()}")
        df_estrangeiro.show(5, truncate=False)

        # 📌 Visão 3: Empresas marcadas como documento alvo
        print("\n📌 Visão: Empresas marcadas como documento alvo")
        df_doc_alvo = df.filter(col("doc_alvo") == True)
        print(f"Total marcadas como doc_alvo: {df_doc_alvo.count()}")
        df_doc_alvo.show(5, truncate=False)



    except Exception as e:
        print(f"❌ Erro ao ler ou processar tabela 'final_result': {e}")

if __name__ == "__main__":
    show_deltagold()