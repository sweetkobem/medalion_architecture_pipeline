# medalion_architecture_pipeline
Medalion architecture pipeline to build a data lakehouse, with the help of Apache Airflow as the orchestrator and pipeline engine, and DuckDB as the data warehouse and for data transformation

## Requirement ##
- Python > 3.8
- Apache Airflow
- DuckDB
- [DAG Generator](https://github.com/sweetkobem/airflow_dag_generator)

## Step by step: ##
- Clone repo/download [here](https://github.com/sweetkobem/airflow_dag_generator).
- Clone this repo in your airflow root folder.
- Download this data from mavenalaytics [here](https://mavenanalytics.io/data-playground?order=date_added%2Cdesc&search=Hospital%20Patient%20Records) and unzip into folder "data/hospital_patient_records".
- run python3 dag_generator.py .