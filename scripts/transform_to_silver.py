from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp
import time
import os
import sys

# Configurações de Autenticação do Google Cloud
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "coloque_aqui_o_caminho_da_sua_service_account.json"

#Variaveis
GCP_PROJECT = "projeto-olist-portfolio"
CAMINHO_CHAVE = "coloque_aqui_o_caminho_da_sua_service_account.json"
GCS_TEMP_BUCKET = "staging-airflow-spark"

# Config do PySpark, não precisamos passar a string de caminhos
spark = SparkSession.builder \
    .appName("Olist_SQLServer_to_BigQuery_Bronze") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.key.file", CAMINHO_CHAVE) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#Funções de transformação
def write_to_silver(df, table_name):
    df.write \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT}.silver_olist.{table_name}") \
        .option("temporaryGcsBucket", GCS_TEMP_BUCKET) \
        .mode("overwrite") \
        .save()
    print(f"✅ Tabela '{table_name}' gravada na Silver.")


def processar_silver_order_items():
    df = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.bronze_olist.order_items").load()
    df_clean = df.withColumn("price", col("price").cast("double")) \
                 .withColumn("freight_value", col("freight_value").cast("double")) \
                 .dropDuplicates(["order_id", "order_item_id"])
    write_to_silver(df_clean, "order_items")
    

def processar_silver_orders():  
    df = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.bronze_olist.orders").load()

    # Limpeza: converter datas e remover duplicados
    df_clean = df.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
                 .dropDuplicates(["order_id"])
    write_to_silver(df_clean, "orders")


def processar_silver_customers():
    df = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.bronze_olist.customers").load()
    
    # Limpeza: garantindo que o zip code seja string e removendo duplicados
    df_clean = df.withColumn("customer_zip_code_prefix", col("customer_zip_code_prefix").cast("string")) \
                 .dropDuplicates(["customer_id"])
    write_to_silver(df_clean, "customers")

def processar_silver_products():
    df = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.bronze_olist.products").load()
    
    # Limpeza: Preencher categorias nulas com 'other' e remover nulos críticos
    df_clean = df.withColumn("product_category_name", when(col("product_category_name").isNull(), "other").otherwise(col("product_category_name"))) \
                 .dropDuplicates(["product_id"])        
    write_to_silver(df_clean, "products")



# Execução
if __name__ == "__main__":
    processar_silver_customers()
    processar_silver_products()
    processar_silver_orders()
    processar_silver_order_items()
