import sys
from email.policy import default

DAGS_FOLDER = '/Users/benedictraj/airflow-docker/dags'
sys.path.append(DAGS_FOLDER)
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from utilities.sql_functions import generate_create_table_sql ,create_oracle_connection, insert_in_batches
import psycopg2
import pandas as pd
import oracledb
import wrds


# 1. Getting data as DF
# 2. Creating create st
# 3. if table exits skip
# 4. Creating insert st
# 5. Ingest data

def fetch_wrds_data(**context):

    conn = psycopg2.connect(
        host='wrds-pgdata.wharton.upenn.edu',
        port=9737,
        dbname='wrds',
        user='braj',
        password='Benedict@1198',
        sslmode='require'
    )
    sql = """SELECT * FROM comp_na_daily_all.company"""
    # conn = wrds_hook.get_conn()
    # conn.set_session(sslmode='require')
    df = pd.read_sql(sql, conn)
    # Push dataframe to XCom as JSON (lightweight version)
    print("here it is",df)
    # context['ti'].xcom_push(key='wrds_data', value=df.to_json())
    file_path = f"/tmp/wrds_data_{context['run_id']}.parquet"
    df.to_parquet(file_path)
    context['ti'].xcom_push(key="wrds_data_path", value=file_path)


def create_table_query(**context):
    path = context['ti'].xcom_pull(key='wrds_data_path')
    if path is None:
        raise ValueError("No WRDS data received!")
    df = pd.read_parquet(path)

    conn = create_oracle_connection(user='RAW_TBS')

    cursor = conn.cursor()
    cursor.execute(""" SELECT COUNT(*)
            FROM ALL_TABLES
            WHERE TABLE_NAME = 'COMPANY_LIST_RAW'
              AND OWNER = UPPER('RAW_TBS') """)
    table_exists = cursor.fetchone()[0]
    print(table_exists)
    if table_exists:
        print("Table already exists. Skipping creation.")
    else:
        create_stmt = generate_create_table_sql(df, 'COMPANY_LIST_RAW')
        print("Table does not exist. Creating table...")
        print(create_stmt)
        cursor.execute(create_stmt)
        conn.commit()
        print("Table created.")

def insert_rows_query(**context):
    path = context['ti'].xcom_pull(key='wrds_data_path')
    if path is None:
        raise ValueError("No WRDS data received!")

    df = pd.read_parquet(path)

    conn = create_oracle_connection('RAW_TBS')
    # insert_rows_one_by_one(df, 'COMP_NA_FUND_Q_RAW',conn)
    insert_in_batches(df,'COMPANY_LIST_RAW',conn,10)




with DAG("wrds_company_raw_ing", start_date=datetime(2025,4,28),schedule_interval="@monthly",catchup=False, tags=["INGESTION","WRDS"]) as dag:
    get_raw_data = PythonOperator(
        task_id = "get_raw_data",
        python_callable = fetch_wrds_data,
        provide_context=True
    )

    create_table = PythonOperator(
        task_id = "create_table",
        python_callable = create_table_query,
        provide_context=True
    )
    insert_rows = PythonOperator(
        task_id = "insert_rows",
        python_callable = insert_rows_query,
        provide_context=True
    )


    get_raw_data >> create_table >> insert_rows


