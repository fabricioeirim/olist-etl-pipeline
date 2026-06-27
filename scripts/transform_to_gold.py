from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp
import time
import os
import sys

# Configurações de Autenticação do Google Cloud
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cole_aqui_sua_service_account.json"

#Variaveis
GCP_PROJECT = "projeto-olist-portfolio"
CAMINHO_CHAVE = "cole_aqui_sua_service_account.json"
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
# ... (Mantenha as configurações globais: GCP_PROJECT, GCS_TEMP_BUCKET, etc.)

def processar_gold_vendas():
    print("🚀 Iniciando processamento da Camada Ouro (Gold)...")

    # 1. Leitura das tabelas que já estão limpas na camada Silver
    df_orders = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.silver_olist.orders").load()
    df_items = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.silver_olist.order_items").load()
    df_products = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.silver_olist.products").load()
    df_customers = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.silver_olist.customers").load()

    # 2. Joins para desnormalizar o dado
    # O foco aqui é transformar o modelo relacional em uma "Flat Table"
    df_gold = df_orders \
        .join(df_items, on="order_id", how="inner") \
        .join(df_products, on="product_id", how="inner") \
        .join(df_customers, on="customer_id", how="inner") \
        .select(
            "order_id",
            "customer_unique_id",
            "order_status",
            "order_purchase_timestamp",
            "product_category_name",
            "price",
            "freight_value",
            "customer_city",
            "customer_state"
        )
    
    # 3. Escrita na Camada Ouro
    df_gold.write \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT}.gold_olist.fato_vendas") \
        .option("temporaryGcsBucket", GCS_TEMP_BUCKET) \
        .mode("overwrite") \
        .save()

    print("✅ Tabela 'fato_vendas' (Gold) criada com sucesso!")

if __name__ == "__main__":
    processar_gold_vendas()
