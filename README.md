Olist Data Engineering Project
Projeto de Engenharia de Dados que implementa uma arquitetura Medallion (Lakehouse) para processar dados de e-commerce da Olist, saindo de um banco SQL Server transacional até um dashboard analítico no Google Looker Studio.

🏛 Arquitetura do Projeto
(Insira aqui seu diagrama)

O pipeline foi projetado para ser modular e resiliente, permitindo que o Apache Airflow orquestre cada etapa de forma independente.

🛠 Tecnologias Utilizadas
Orquestração: Apache Airflow

Processamento: Apache Spark (PySpark)

Data Warehouse: Google BigQuery

Infraestrutura: Docker & Docker Compose

💡 Decisões de Arquitetura (O "Porquê")
Por que Apache Spark?
Embora o volume inicial de dados seja de 100 mil linhas, a escolha do PySpark em detrimento de bibliotecas como Pandas foi estratégica para garantir a escalabilidade vertical e horizontal. O processamento pseudo-distribuído do Spark permite que o pipeline lide com milhões de registros de forma eficiente, preparando a infraestrutura para o crescimento esperado da volumetria de dados sem a necessidade de reescrever o código.

Por que a Arquitetura Medallion?
Adotamos a arquitetura Medallion (Bronze/Silver/Gold) não apenas por ser uma melhor prática de mercado, mas por dois motivos técnicos cruciais:

Modularidade: A separação física e lógica das camadas facilita a identificação e isolamento de falhas, permitindo reprocessar etapas específicas do pipeline sem afetar o fluxo completo.

Orquestração Inteligente: Com o Airflow, tratamos cada camada como uma tarefa independente, o que possibilita um controle granular, re-tentativas (retries) otimizadas e uma governança de dados mais clara.

🚀 Como rodar
Clone este repositório.

Configure as credenciais da GCP na pasta eng_dados/.

Suba o ambiente via docker-compose up -d.

Dispare a DAG pipeline_olist_etl pelo painel do Airflow.

📈 Arquitetura

<img width="1535" height="1024" alt="image" src="https://github.com/user-attachments/assets/cacfc20a-dbbb-4ff5-ac0e-97681011f055" />


📈 Resultados

<img width="1095" height="557" alt="image" src="https://github.com/user-attachments/assets/bb73ddb1-9422-44e9-aa38-f1ac372913b9" />
