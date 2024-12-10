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
    source = airflow_home_path + '/data/medalion_dag/slv_payers/' + execution_date

    table = 'dim_payer'
    key_source = 'payer_id'
    key_destination = 'dim_payer_key'
    schema = {
        'payer_id': 'VARCHAR NOT NULL',
        'payer_name': 'VARCHAR NOT NULL',
        'address': 'TEXT',
        'city': 'VARCHAR',
        'zip': 'INTEGER',
        'start_date': 'DATE NOT NULL',
        'end_date': 'DATE',
        'is_active': 'BOOLEAN'
    }
    columns = [column for column in schema.keys()]
    filtered_column = gnrl.get_filtered_column(key_destination, columns)
    md5_column = gnrl.encoded_column_by_sql(filtered_column)

    # Open connection to duckdb
    conn = duckdb.connect(airflow_home_path+"/medalion.duckdb")

    # Check table exist
    is_table_exists = gnrl.is_table_exists(conn, table)

    # Get last date of table
    last_date = None
    if is_table_exists:
        last_date = gnrl.get_last_date(conn, table)

    if last_date is not None:
        # Mark failed data because data in this execution date exist
        if last_date >= execution_date:
            print('Data already exists for this execution date! Please delete the data for this date and any later dates in the table if you want to backfill.')
            sys.exit(1)

    # Get data from source and create temporary file in duckdb
    exclude_keys = ['start_date', 'end_date', 'is_active']
    schema_sql = gnrl.schema_dict_to_sql(schema, exclude_keys)
    sql = f"""
        CREATE TEMPORARY TABLE temp_{table} (
            {schema_sql}
        );
        INSERT INTO temp_{table}
        SELECT * FROM '{source}/*.parquet'
    """
    conn.execute(sql)

    # Create persistence table in DuckDB
    if is_table_exists is False:
        schema_sql = gnrl.schema_dict_to_sql(schema)
        conn.execute(f"""
        CREATE SEQUENCE {key_destination} START 1;
        CREATE TABLE {table} (
            {key_destination} INTEGER PRIMARY KEY DEFAULT nextval('{key_destination}'),
            {schema_sql}
        );
        CREATE INDEX idx_{table}_is_active ON {table}(is_active);
        CREATE INDEX idx_{table}_start_date ON {table}(start_date);
        CREATE INDEX idx_{table}_end_date ON {table}(end_date);""")

    # Get changes data
    data = gnrl.dim_table_get_changes_data(
        conn, execution_date, table, key_destination, key_source, md5_column)

    # Update latest data and store by changes data
    if not data.empty:
        gnrl.dim_table_store_data(
            conn, execution_date, table, key_destination, columns, data)
        print(f"Success store data to table {table}")
    else:
        print("No data stored.")

    conn.close()


if __name__ == '__main__':
    execution_date = sys.argv[1].replace('T', ' ')
    airflow_connection = pickle.loads(base64.b64decode(sys.argv[2]))
    airflow_variable = pickle.loads(base64.b64decode(sys.argv[3]))

    main(execution_date, airflow_connection, airflow_variable)
