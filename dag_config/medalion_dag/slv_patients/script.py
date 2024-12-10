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
    source = airflow_home_path + '/data/medalion_dag/brz_patients/' + execution_date
    destination = airflow_home_path + '/data/medalion_dag/slv_patients/' + execution_date

    if not os.path.exists(destination):
        os.makedirs(destination)

    conn = duckdb.connect()
    query = f"""SELECT
            {clnr.clean_string('id')} AS patient_id,
            ({clnr.capitalized_case('FIRST')} || {clnr.capitalized_case('LAST')}) AS patient_fullname,
            BIRTHDATE AS birth_date,
            DEATHDATE AS death_date,
            {clnr.marital_status('MARITAL')} AS marital_status,
            {clnr.clean_string('RACE')} AS race,
            {clnr.clean_string('ETHNICITY')} AS ethnicity,
            {clnr.gender('GENDER')} AS gender,
            {clnr.clean_string('BIRTHPLACE')} AS birth_place,
            {clnr.clean_string('ADDRESS')} AS address,
            {clnr.clean_string('CITY')} AS city,
            {clnr.clean_string('STATE')} AS state,
            {clnr.clean_string('COUNTY')} AS county,
            {clnr.clean_string('ZIP')} AS zip
        FROM '{source}/patients.parquet'
        """
    conn.execute(f"COPY ({query}) TO '{destination}/patients.parquet' (FORMAT PARQUET)")
    conn.close()


if __name__ == '__main__':
    execution_date = sys.argv[1].replace('T', ' ')
    airflow_connection = pickle.loads(base64.b64decode(sys.argv[2]))
    airflow_variable = pickle.loads(base64.b64decode(sys.argv[3]))

    main(execution_date, airflow_connection, airflow_variable)