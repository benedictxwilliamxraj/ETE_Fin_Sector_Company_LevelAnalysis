import sys
from email.policy import default
import os
import gc
DAGS_FOLDER = '/Users/benedictraj/airflow-docker/dags'
sys.path.append(DAGS_FOLDER)
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from utilities.sql_functions import generate_create_table_sql ,create_oracle_connection,get_oracle_schema, match_types_to_oracle,insert_in_batches
from airflow.providers.oracle.operators.oracle import OracleOperator
import psycopg2
import pandas as pd
import numpy as np
import oracledb
import wrds
OUTPUT_DIR = "/tmp/wrds_chunks"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# 1. Getting data as DF
# 2. Creating create st
# 3. if table exits skip
# 4. Creating insert st
# 5. Ingest data
CHUNK_SIZE = 40000

def fetch_wrds_data(**context):
    conn = psycopg2.connect(
        host='wrds-pgdata.wharton.upenn.edu',
        port=9737,
        dbname='wrds',
        user='braj',
        password='Benedict@1198',
        sslmode='require'
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM comp_na_daily_all.fundq WHERE datadate >= '2020-01-01'")
    MAX_ROWS = cur.fetchone()[0]
    print(f"Total rows to fetch: {MAX_ROWS}")

    offset = 0
    total_rows = 0
    chunk_index = 0
    chunks = []

    while total_rows < MAX_ROWS:
        query = f"""
            SELECT * FROM comp_na_daily_all.fundq
            WHERE datadate >= '2020-01-01'
            ORDER BY gvkey, datadate
            LIMIT {CHUNK_SIZE} OFFSET {offset}
        """
        df = pd.read_sql(query, conn)

        if df.empty:
            print("No more data to fetch.")
            break

        chunks.append(df)
        print(f"Fetched {len(df)} rows in chunk {chunk_index}")

        offset += CHUNK_SIZE
        total_rows += len(df)
        chunk_index += 1

        del df
        gc.collect()

    conn.close()

    final_df = pd.concat(chunks, ignore_index=True)
    file_path = f"/tmp/wrds_data_{context['run_id']}.parquet"
    final_df.to_parquet(file_path, index=False)
    print('Data written to:', file_path)

    context['ti'].xcom_push(key="wrds_data_path", value=file_path)

def create_table_query(**context):
    # wrds_data_json = context['ti'].xcom_pull(task_ids='get_raw_data', key='wrds_data')
    # if wrds_data_json is None:
    #     raise ValueError("No WRDS data received!")
    # df = pd.read_json(wrds_data_json)
    path = context['ti'].xcom_pull(key='wrds_data_path')
    if path is None:
        raise ValueError("No WRDS data received!")
    df = pd.read_parquet(path)

    conn = create_oracle_connection(user='RAW_TBS')

    cursor = conn.cursor()
    cursor.execute(""" SELECT COUNT(*)
            FROM ALL_TABLES
            WHERE TABLE_NAME = 'COMP_NA_FUND_Q_RAW'
              AND OWNER = UPPER('RAW_TBS') """)
    table_exists = cursor.fetchone()[0]
    print(table_exists)
    if table_exists:
        print("Table already exists. Skipping creation.")
    else:
        create_stmt = generate_create_table_sql(df, 'COMP_NA_FUND_Q_RAW')
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
    cursor = conn.cursor()

    table_name = 'COMP_NA_FUND_Q_RAW'
    oracle_schema = get_oracle_schema(cursor, table_name)
    df = match_types_to_oracle(df, oracle_schema)

    columns = df.columns.tolist()
    placeholders = ', '.join([f":{col}" for col in columns])
    columns_str = ', '.join(columns)

    sql = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
    """

    # BATCH_SIZE = len(df)-50000
    BATCH_SIZE = 40000
    #start = 0
    for start in range(0, len(df), BATCH_SIZE):
    # while start < len(df):
        # if start + BATCH_SIZE > len(df):
        #     chunk = df.iloc[start:].copy()
        # else:
        chunk = df.iloc[start:start + BATCH_SIZE].copy()

        # Apply type coercion AGAIN inside the chunk to ensure per-batch safety
        for col, dtype in oracle_schema.items():
            if col not in chunk.columns:
                continue

            if dtype.startswith('NUMBER'):
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
            elif dtype.startswith('VARCHAR') or dtype.startswith('CHAR'):
                chunk[col] = chunk[col].astype(str).replace({'nan': None, '': None})
            elif dtype.startswith('DATE'):
                chunk[col] = pd.to_datetime(chunk[col], errors='coerce')

        chunk = chunk.replace({pd.NA: None, np.nan: None})

        try:
            records = chunk.to_dict(orient='records')
            cursor.executemany(sql, records)
            conn.commit()
            print(f" Inserted rows {start} to {start + len(records) - 1}")
            start += BATCH_SIZE
        except Exception as e:
            print(f" Error in batch starting at row {start}: {e}")
            print("Started inserting batchwise")
            insert_in_batches(chunk,table_name, conn, 40)
            # print(chunk.iloc[0])

            #conn.rollback()
        # start += BATCH_SIZE

    cursor.close()
    conn.close()





with DAG("wrds_comp_na_fundq", start_date=datetime(2025,4,28),schedule_interval="@monthly",catchup=False, tags=["INGESTION","WRDS"]) as dag:
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


