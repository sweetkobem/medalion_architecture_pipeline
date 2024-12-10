import sys
import base64
import pickle
import duckdb
import os
from datetime import datetime

# Add folder pludgins/functions and import
paths = os.path.dirname(os.path.abspath(__file__)).split('/')
index_path = paths.index('dag_config')
sys.path.append(f"{'/'.join(paths[0:index_path])}/plugins")
from functions import general as gnrl


def main(execution_date, airflow_connection, airflow_variable):
    # Full load method
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S').date()
    execution_date = execution_date.strftime('%Y-%m-%d')

    airflow_home_path = airflow_variable['airflow_home_path']

    table = 'daily_encounter'
    schema = {
        'date': 'DATE NOT NULL',
        'dim_patient_key': 'VARCHAR NOT NULL',
        'dim_payer_key': 'VARCHAR NOT NULL',
        'total_encounter_num': 'INTEGER',
        'total_procedure_num': 'INTEGER',
        'total_diagnosis_num': 'INTEGER',
        'total_claim_amount': 'FLOAT',
        'total_coverage_amount': 'FLOAT',
        'total_patient_pay_amount': 'FLOAT'
    }
    columns = [column for column in schema.keys()]

    # Open connection to duckdb
    conn = duckdb.connect(airflow_home_path+"/medalion.duckdb")

    # Check table exist
    is_table_exists = gnrl.is_table_exists(conn, table)

    # Get total data in execution date
    total_data = 0
    if is_table_exists:
        total_data = gnrl.get_total_data_in_execution_date(
            conn, table, 'date', execution_date)

    if total_data > 0:
        # Mark failed because data in this execution date exist
        print('Data already exists for this execution date! Please delete the data for this date in the table if you want to backfill.')
        sys.exit(1)

    # Create persistence table in DuckDB
    if is_table_exists is False:
        exclude_keys = []
        schema = {k: v for k, v in schema.items() if k not in exclude_keys}
        schema_sql = gnrl.schema_dict_to_sql(schema)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {schema_sql},
            PRIMARY KEY (date, dim_patient_key, dim_payer_key)
        );""")

    # Transform data
    sql = f"""
        SELECT
            source.date,
            source.dim_patient_key,
            source.dim_payer_key,
            COUNT(DISTINCT source.encounter_id) AS total_encounter_num,
            COUNT(fact_procedure.encounter_id) AS total_procedure_num,
            COUNT(fact_procedure.code) AS total_diagnosis_num,
            SUM(source.claim_amount) AS total_claim_amount,
            SUM(source.coverage_amount) AS total_coverage_amount,
            SUM(source.patient_pay_amount) AS total_patient_pay_amount

        FROM fact_encounter source
        LEFT JOIN fact_procedure
            ON (source.encounter_id=fact_procedure.encounter_id
                AND source.date=fact_procedure.date)

        WHERE source.date = '{execution_date}'
        GROUP BY ALL
    """
    data = conn.execute(sql).fetchdf()

    # Store data
    if not data.empty:
        exclude_keys = []
        columns = [v for v in columns if v not in exclude_keys]
        gnrl.fact_table_store_data(
            conn, table, columns, data)
        print(f"Success store data to table {table}")
    else:
        print("No data stored.")

    conn.close()


if __name__ == '__main__':
    execution_date = sys.argv[1].replace('T', ' ')
    airflow_connection = pickle.loads(base64.b64decode(sys.argv[2]))
    airflow_variable = pickle.loads(base64.b64decode(sys.argv[3]))

    main(execution_date, airflow_connection, airflow_variable)
