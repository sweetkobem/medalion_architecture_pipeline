import sys
import base64
import pickle
import duckdb
import os
from datetime import datetime


def main(execution_date, airflow_connection, airflow_variable):
    # Incremental method
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S').date()
    execution_date = execution_date.strftime('%Y-%m-%d')

    airflow_home_path = airflow_variable['airflow_home_path']
    source = airflow_home_path + '/data/hospital_patient_records'
    destination = airflow_home_path + '/data/medalion_dag/brz_procedures/' + execution_date

    if not os.path.exists(destination):
        os.makedirs(destination)

    conn = duckdb.connect()
    query = f"""SELECT * FROM '{source}/procedures.csv'
            WHERE (strftime('%Y-%m-%d', start) = '{execution_date}' OR strftime('%Y-%m-%d', stop) = '{execution_date}')"""
    print(query)
    conn.execute(f"COPY ({query}) TO '{destination}/procedures.parquet' (FORMAT PARQUET)")
    conn.close()


if __name__ == '__main__':
    execution_date = sys.argv[1].replace('T', ' ')
    airflow_connection = pickle.loads(base64.b64decode(sys.argv[2]))
    airflow_variable = pickle.loads(base64.b64decode(sys.argv[3]))

    main(execution_date, airflow_connection, airflow_variable)
