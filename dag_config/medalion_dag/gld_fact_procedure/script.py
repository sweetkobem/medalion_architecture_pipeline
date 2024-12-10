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
    source = airflow_home_path + '/data/medalion_dag/slv_procedures/' + execution_date

    table = 'fact_procedure'
    partition_key = 'stop_time'
    schema = {
        'date': 'DATE NOT NULL',
        'dim_patient_key': 'VARCHAR NOT NULL',
        'patient_id': 'VARCHAR NOT NULL',
        'encounter_id': 'VARCHAR NOT NULL',
        'code': 'VARCHAR',
        'description': 'VARCHAR',
        'reason_code': 'VARCHAR',
        'reason_description': 'TEXT',
        'start_time': 'DATETIME',
        'stop_time': 'DATETIME',
        'base_cost_amount': 'FLOAT',
        'duration_minute_num': 'INTEGER'
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
            conn, table, 'stop_time', execution_date)

    if total_data > 0:
        # Mark failed because data in this execution date exist
        print('Data already exists for this execution date! Please delete the data for this date in the table if you want to backfill.')
        sys.exit(1)

    # Get data from source and create temporary file in duckdb
    exclude_keys = ['date', 'dim_patient_key', 'duration_minute_num']
    schema_sql = gnrl.schema_dict_to_sql(schema, exclude_keys)
    columns_temp = [v for v in columns if v not in exclude_keys]
    sql = f"""
        CREATE TEMPORARY TABLE temp_{table} (
            {schema_sql}
        );
        INSERT INTO temp_{table}
        SELECT {','.join(columns_temp)} FROM '{source}/*.parquet'
    """
    conn.execute(sql)

    # Create persistence table in DuckDB
    if is_table_exists is False:
        exclude_keys = ['patient_id']
        schema = {k: v for k, v in schema.items() if k not in exclude_keys}
        schema_sql = gnrl.schema_dict_to_sql(schema)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {schema_sql},
            PRIMARY KEY(encounter_id, code)
        );
        CREATE INDEX idx_{table}_date ON {table}(date);
        CREATE INDEX idx_{table}_code ON {table}(code);
        CREATE INDEX idx_{table}_dim_patient_key ON {table}(dim_patient_key);
        CREATE INDEX idx_{table}_encounter_id ON {table}(encounter_id);
        CREATE INDEX idx_{table}_start_time ON {table}(start_time);
        CREATE INDEX idx_{table}_stop_time ON {table}(stop_time);""")

    # Transform data
    sql = f"""
        SELECT
            source.stop_time::DATE AS date,
            dim_patient.dim_patient_key,
            source.encounter_id,
            source.code,
            source.description,
            source.reason_code,
            source.reason_description,
            source.start_time,
            source.stop_time,
            source.base_cost_amount,
            (EXTRACT(EPOCH FROM stop_time - start_time)/60) AS duration_minute_num

        FROM temp_{table} source
        LEFT JOIN dim_patient
            ON (source.patient_id=dim_patient.patient_id
                AND dim_patient.start_date <= '{execution_date}'
                AND (dim_patient.end_date > '{execution_date}'
                    OR dim_patient.is_active IS TRUE))

        WHERE source.{partition_key}::DATE = '{execution_date}'
    """
    data = conn.execute(sql).fetchdf()

    # Store data
    if not data.empty:
        exclude_keys = ['patient_id']
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
