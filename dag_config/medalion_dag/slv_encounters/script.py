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
    source = airflow_home_path + '/data/medalion_dag/brz_encounters/' + execution_date
    destination = airflow_home_path + '/data/medalion_dag/slv_encounters/' + execution_date

    if not os.path.exists(destination):
        os.makedirs(destination)

    conn = duckdb.connect()
    query = f"""SELECT
            {clnr.clean_string('id')} AS encounter_id,
            START AS start_time,
            STOP AS stop_time,
            {clnr.clean_string('PATIENT')} AS patient_id,
            {clnr.clean_string('PAYER')} AS payer_id,
            {clnr.upper_case('ENCOUNTERCLASS')} AS encounter_class,
            CODE AS code,
            {clnr.clean_string('DESCRIPTION')} AS description,
            BASE_ENCOUNTER_COST AS base_cost_amount,
            TOTAL_CLAIM_COST AS claim_amount,
            PAYER_COVERAGE AS coverage_amount,
            REASONCODE AS reason_code,
            {clnr.clean_string('REASONDESCRIPTION')} AS reason_description

        FROM '{source}/encounters.parquet'
        """
    conn.execute(f"COPY ({query}) TO '{destination}/encounters.parquet' (FORMAT PARQUET)")
    conn.close()


if __name__ == '__main__':
    execution_date = sys.argv[1].replace('T', ' ')
    airflow_connection = pickle.loads(base64.b64decode(sys.argv[2]))
    airflow_variable = pickle.loads(base64.b64decode(sys.argv[3]))

    main(execution_date, airflow_connection, airflow_variable)
