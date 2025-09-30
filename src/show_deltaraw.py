from pyspark.sql import SparkSession

def show_deltaraw():
    """Valida a leitura das tabelas Delta na camada BRONZE."""
    
    # --- CONFIGURAÇÕES DE DEPENDÊNCIA ---
    # A dependência Delta Lake será resolvida pelo CMD do Docker,
    # mas se rodarmos o arquivo isoladamente, precisamos desta configuração.
    DELTA_PACKAGES = "io.delta:delta-core_2.12:3.1.0"
    DELTA_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
    DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    # Inicializa Spark com suporte ao Delta Lake
    spark = SparkSession.builder \
        .appName("ValidationDeltaRaw") \
        .config("spark.jars.packages", DELTA_PACKAGES) \
        .config("spark.sql.extensions", DELTA_EXTENSIONS) \
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # Lista de tabelas Delta que você quer validar
    tabelas = [
        "Socios",
        "Empresas"
    ]

    # Caminho base onde as tabelas foram salvas
    base_path = "data/raw"

    # Loop para leitura e exibição
    for tabela in tabelas:
        caminho = f"{base_path}/{tabela}"
        print(f"\nLendo tabela Delta: {tabela}")
        try:
            df = spark.read.format("delta").load(caminho)
            print(f"Tabela '{tabela}' lida com sucesso. Total de registros: {df.count()}")
            df.printSchema()
            df.show(5, truncate=False)
        except Exception as e:
            print(f"Erro ao ler tabela '{tabela}': {e}")

if __name__ == "__main__":
    show_deltaraw()