import pyspark
print(pyspark.__version__)

# %%
from pyspark.sql import SparkSession
import os
import sys

# %%
# 1. Configurações de Autenticação do Google Cloud
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"
KEY_PATH = "aqui_coloque_sua_service_account.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH
GCP_PROJECT = "projeto-olist-portfolio"
CAMINHO_CHAVE = "aqui_coloque_sua_service_account.json"
GCS_TEMP_BUCKET = "staging-airflow-spark"

# %%
# Como você copiou os JARs para a pasta interna do PySpark, não precisamos passar a string de caminhos
spark = SparkSession.builder \
    .appName("Olist_SQLServer_to_BigQuery_Bronze") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.key.file", CAMINHO_CHAVE) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# %%
# 3. Configurações de Conexão com o SQL Server (Local Linux)
#    
jdbc_url = "jdbc:sqlserver://172.17.0.1:1433;databaseName=Olist_Transacional;encrypt=true;trustServerCertificate=true;"
connection_properties = {
    "user": "seu_usuario_sqlserver",
    "password": "suasenha",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# %%
def extrair_tabela_para_bronze(nome_tabela):
    print(f"📥 Lendo tabela '{nome_tabela}' do SQL Server...")
    
    # READ: Spark lê os dados via JDBC
    df_raw = spark.read.jdbc(
        url=jdbc_url, 
        table=nome_tabela, 
        properties=connection_properties
    )
    
    from pyspark.sql.functions import current_timestamp, input_file_name
    df_bronze = df_raw.withColumn("extracted_at", current_timestamp())

        
    print(f"✅ Tabela '{nome_tabela}' enviada com sucesso!")
    df_bronze.write \
        .format("bigquery") \
        .option("parentProject", GCP_PROJECT) \
        .option("table", f"{GCP_PROJECT}.bronze_olist.{nome_tabela}") \
        .option("temporaryGcsBucket", GCS_TEMP_BUCKET) \
        .mode("overwrite") \
        .save()
        
    print(f"✅ Tabela '{nome_tabela}' enviada com sucesso!")

# %%
if __name__ == "__main__":
    # Exemplo com algumas tabelas que você populou no passo anterior
    tabelas_olist = ["products", "orders", "customers", "order_items"]
    
    for tabela in tabelas_olist:
        extrair_tabela_para_bronze(tabela)
        
    spark.stop()
