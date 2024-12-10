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
from functions import cleaner as clnr


def main(execution_date, airflow_connection, airflow_variable):
    # Full load method
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S').date()
    execution_date = execution_date.strftime('%Y-%m-%d')

    airflow_home_path = airflow_variable['airflow_home_path']
    source = airflow_home_path + '/data/medalion_dag/brz_payers/' + execution_date
    destination = airflow_home_path + '/data/medalion_dag/slv_payers/' + execution_date

    if not os.path.exists(destination):
        os.makedirs(destination)

    conn = duckdb.connect()
    query = f"""SELECT
            {clnr.clean_string('id')} AS payer_id,
            NAME AS payer_name,
            {clnr.clean_string('ADDRESS')} AS address,
            {clnr.clean_string('CITY')} AS city,
            ZIP AS zip

        FROM '{source}/payers.parquet'
        """
    conn.execute(f"COPY ({query}) TO '{destination}/payers.parquet' (FORMAT PARQUET)")
    conn.close()


if __name__ == '__main__':
    execution_date = sys.argv[1].replace('T', ' ')
    airflow_connection = pickle.loads(base64.b64decode(sys.argv[2]))
    airflow_variable = pickle.loads(base64.b64decode(sys.argv[3]))

    main(execution_date, airflow_connection, airflow_variable)
