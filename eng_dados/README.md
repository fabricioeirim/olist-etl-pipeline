# Dependências e Configurações

Esta pasta contém os arquivos necessários para a execução do pipeline de dados, incluindo os drivers JDBC e conectores necessários para o Apache Spark.

## 📁 Estrutura de Arquivos

### `/jars`
Nesta pasta, você deve incluir os arquivos `.jar` necessários para a integração do Spark com o SQL Server, BigQuery e GCS. 

Os arquivos utilizados neste projeto são:
- `spark-bigquery-with-dependencies_2.12-0.41.0.jar`
- `mssql-jdbc-12.4.2.jre11.jar`
- `gcs-connector-hadoop3-2.2.22-shaded.jar`

> **Nota:** Certifique-se de que as versões dos JARs sejam compatíveis com a versão do seu Apache Spark e do Java rodando no container.

### `/credenciais`
Este diretório é destinado às chaves de autenticação do Google Cloud Platform.

> ⚠️ **IMPORTANTE:** Por questões de segurança, **nunca** envie seus arquivos `.json` de Service Account para o repositório remoto. 
> 
> Mantenha seus arquivos `.json` localmente nesta pasta e adicione-os ao seu `.gitignore` para evitar vazamentos de dados sensíveis.
