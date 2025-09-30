# 🧱 Imagem base com Spark 3.4.1, PySpark, Java e Jupyter
FROM jupyter/pyspark-notebook:spark-3.4.1

# 📁 Diretório de trabalho
WORKDIR /home/jovyan/work/app

# 🐍 Variável de ambiente para o Python encontrar seu código
ENV PYTHONPATH="$PYTHONPATH:/home/jovyan/work/app"

# 📦 Instala dependências Python adicionais
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 📂 Copia o restante da aplicação
COPY . /home/jovyan/work/app

# 🚀 Comando final: executa o Spark com suporte ao Delta Lake
CMD bash -c "spark-submit \
    --packages io.delta:delta-core_2.12:2.4.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    src/main.py"
