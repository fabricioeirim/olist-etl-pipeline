from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Argumentos padrão para a DAG
default_args = {
    'owner': 'fabricio',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Parâmetros comuns do Spark para todas as camadas
SPARK_COMMON_ARGS = (
    "--master local[*] "
    "--jars /opt/airflow/meuprojeto/eng_dados/spark-bigquery-with-dependencies_2.12-0.41.0.jar,"
    "/opt/airflow/meuprojeto/eng_dados/mssql-jdbc-12.4.2.jre11.jar,"
    "/opt/airflow/meuprojeto/eng_dados/gcs-connector-hadoop3-2.2.22-shaded.jar "
    '--conf "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" '
    '--conf "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" '
)

with DAG(
    'pipeline_olist_etl',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    tags=['olist', 'etl', 'bigquery']
) as dag:

    # 1. Extração (Bronze)
    extrair_bronze = BashOperator(
        task_id='extrair_bronze',
        bash_command=f'spark-submit {SPARK_COMMON_ARGS} /opt/airflow/meuprojeto/script/extract_to_bronze_valida.py'
    )

    # 2. Transformação (Silver)
    transformar_silver = BashOperator(
        task_id='transformar_silver',
        bash_command=f'spark-submit {SPARK_COMMON_ARGS} /opt/airflow/meuprojeto/script/transform_to_silver_v1.py'
    )

    # 3. Agregação (Gold)
    transformar_gold = BashOperator(
        task_id='transformar_gold',
        bash_command=f'spark-submit {SPARK_COMMON_ARGS} /opt/airflow/meuprojeto/script/trasform_to_gold.py'
    )

    # Definindo a dependência: Bronze -> Silver -> Gold
    extrair_bronze >> transformar_silver >> transformar_gold
