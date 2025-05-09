import numpy as np
import pandas as pd
import oracledb

def create_oracle_connection(user):
    # session = settings.Session()
    # conn_id = 'oracle_conn'

    # dsn = "adb.us-ashburn-1.oraclecloud.com/gf3951d1afb8c0b_fundamentalraw_high.adb.oraclecloud.com"

    conn = oracledb.connect(
        user= user,
        password="MSBAFinancial@2025",
        dsn="fundamentalraw_high",
        wallet_password="Benedict@98",
        host=1522,

    )

    return conn


def generate_create_table_sql(df, table_name):


    def map_dtype(series):
        """Map pandas series to Oracle SQL datatype"""
        if pd.api.types.is_datetime64_any_dtype(series):
            return "DATE"
        elif pd.api.types.is_integer_dtype(series):
            return "INT"
        elif pd.api.types.is_float_dtype(series):
            return "FLOAT"
        else:
            return "VARCHAR2(4000)"

    columns_sql = []
    for col in df.columns:
        col_name = col.upper().strip().replace(" ", "_")
        col_type = map_dtype(df[col])
        columns_sql.append(f'"{col_name}" {col_type}')

    create_stmt = f'CREATE TABLE "{table_name.upper()}" (  ' + ', '.join(columns_sql) + ')'
    return create_stmt



def get_oracle_schema(cursor, table_name):
    cursor.execute(f"""
        SELECT column_name, data_type
        FROM user_tab_columns
        WHERE table_name = UPPER(:1)
        ORDER BY column_id
    """, [table_name])
    return dict(cursor.fetchall())  # {col_name: data_type}



def match_types_to_oracle(df, oracle_schema):
    for col, dtype in oracle_schema.items():
        if col not in df.columns:
            continue

        if dtype.startswith('NUMBER'):
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif dtype.startswith('VARCHAR') or dtype.startswith('CHAR'):
            # Ensure floats are converted to string if present
            df[col] = df[col].astype(str).replace({'nan': None, '': None})
        elif dtype.startswith('DATE'):
            df[col] = pd.to_datetime(df[col], errors='coerce')

    df = df.replace({pd.NA: None, np.nan: None})
    return df


def generate_insert_all_sql(df, table_name):
    """
    Generate an INSERT ALL SQL statement for Oracle from a cleaned DataFrame.
    """
    def clean_value(x):
        if pd.isna(x) or (isinstance(x, str) and x.strip() == ''):
            return 'NULL'
        elif isinstance(x, (np.floating, float, np.integer, int)):
            return str(x)
        else:
            return f"'{str(x).replace("'", "''")}'"


    # Clean column names
    columns = [col.upper().strip().replace(" ", "_") for col in df.columns]
    columns_str = ", ".join(f'"{col}"' for col in columns)

    # Clean and format each row
    cleaned_rows = []
    for _, row in df.iterrows():
        cleaned = [clean_value(val) for val in row]
        cleaned_rows.append(f"INTO {table_name.upper()} ({columns_str}) VALUES ({', '.join(cleaned)})")

    # Combine all parts
    insert_all_sql = "INSERT ALL " + "".join(cleaned_rows) + " SELECT 1 FROM DUAL"

    return insert_all_sql

def insert_in_batches(df, table_name, connection, batch_size):
    cursor = connection.cursor()

    total_rows = len(df)
    batches = (total_rows + batch_size - 1) // batch_size  # ceil division

    print(f"Total rows: {total_rows}, Batch size: {batch_size}, Total batches: {batches}")

    for batch_num in range(batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total_rows)

        batch_df = df.iloc[start_idx:end_idx]

        insert_sql = generate_insert_all_sql(batch_df, table_name)



        try:
            cursor = connection.cursor()
            cursor.execute(insert_sql)
            connection.commit()
            print(f" Batch {batch_num+1}/{batches} inserted successfully: Rows {start_idx} to {end_idx-1}")
        except Exception as e:
            try:
                connection.ping()
                print("Connection still alive. Rolling back.")
                connection.rollback()
                print('failed') # decrease batch size
                exit()
            except:
                print("Connection is dead. Reconnecting...")
                try:
                    connection = create_oracle_connection('RAW_TBS')
                    cursor = connection.cursor()
                    cursor.execute(insert_sql)
                    connection.commit()
                    print(f"Reconnected and inserted batch {batch_num+1} successfully after reconnect.")
                    cursor.close()
                except Exception as e2:
                    print(f"Failed after reconnect: {e2}")
                    connection.rollback()
                    break

    cursor.close()


def insert_rows_one_by_one(df, table_name, connection):

    cursor = connection.cursor()

    # Clean columns: uppercase and safe for Oracle
    df.columns = [col.upper().strip().replace(" ", "_") for col in df.columns]

    # Build INSERT statement
    columns_str = ", ".join(f'"{col}"' for col in df.columns)
    placeholders = ", ".join(f":{i+1}" for i in range(len(df.columns)))

    insert_sql = f'INSERT INTO "{table_name.upper()}" ({columns_str}) VALUES ({placeholders})'

    for idx in range(len(df)):
        row = df.iloc[idx]

        # Clean row: handle None, float, int, string
        cleaned_row = row.astype(object).apply(
            lambda x: None if pd.isna(x) else
                      float(x) if isinstance(x, (np.floating, float)) else
                      int(x) if isinstance(x, (np.integer, int)) else
                      x
        ).tolist()

        try:
            cursor.execute(insert_sql, cleaned_row)
            connection.commit()
            # print(f" Successfully inserted row {idx}")
        except Exception as e:
            print(f" Error inserting row {idx}: {e}")
            print(row)
            cursor.close()
            return  # Stop further insertions immediately

    cursor.close()
    print(f"All rows inserted successfully into {table_name}")
