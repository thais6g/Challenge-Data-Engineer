from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit
import os
import shutil

spark = None

def get_spark_session():
    """Inicializa ou retorna a SparkSession configurada para Delta Lake."""
    global spark
    if spark is None:
        builder = SparkSession.builder \
            .appName("GoldBuilder") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), 'data', 'spark-warehouse'))

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    return spark

def builder_gold(origem="data/silver", destino="data/gold"):
    """
    Leitura das tabelas presentes na camada Silver. 
    União das tabelas por intersessão, aplicação de regra de negócio e agregações.
    Carga na camada Gold
    """
    spark = get_spark_session()

    os.makedirs(destino, exist_ok=True)
    print("\n--- Construindo tabela Gold ---")

    try:
        # 1. Leitura das Tabelas da Camada Silver
        df_empresas = spark.read.format("delta").load(os.path.join(origem, "Empresas"))
        df_socios = spark.read.format("delta").load(os.path.join(origem, "Socios"))
        print("Tabelas EMPRESAS e SOCIOS lidas da camada Silver.")
    except Exception as e:
        print(f"Erro ao ler tabelas Silver: {e}")
        return

    try:
        # 2. Join direto entre empresas e sócios válidos
        df_base = df_empresas.join(df_socios, "cnpj", "inner")

        # 3. Cálculo dos indicadores diretamente sobre o join
        df_gold = df_base.groupBy("cnpj", "cod_porte").agg(
            count(lit(1)).alias("qtde_socios"),
            (count(when(col("tipo_socio") == 3, 1)) > 0).alias("flag_socio_estrangeiro")
        )

        # 4. Criação do campo doc_alvo
        condicao_doc_alvo = (col("cod_porte") == lit("03")) & (col("qtde_socios") > lit(1))
        df_gold = df_gold.withColumn("doc_alvo", when(condicao_doc_alvo, lit(True)).otherwise(lit(False)))

        # 5. Seleção final de colunas
        df_gold = df_gold.select(
            col("cnpj"),
            col("qtde_socios").cast("int"),
            col("flag_socio_estrangeiro").cast("boolean"),
            col("doc_alvo").cast("boolean")
        )
        print("Tabela GOLD construída com sucesso.")
    except Exception as e:
        print(f"Erro ao construir tabela GOLD: {e}")
        return

    try:
        # 6. Escrita na Camada Gold
        output_path = os.path.join(destino, "final_result")
        df_gold.write.format("delta").mode("overwrite").save(output_path)
        print(f"Tabela salva com sucesso na GOLD em: {output_path}")
    except Exception as e:
        print(f"Erro ao salvar tabela GOLD: {e}")

if __name__ == "__main__":
    builder_gold()