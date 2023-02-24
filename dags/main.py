from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
import json



def read_dataset():
    dfs = []
    chunk_size = 25
    for chunk in pd.read_csv("./data/all_stocks_5yr.csv", chunksize=chunk_size):
        dfs.append(chunk)
    df = pd.concat(dfs, ignore_index = True)
    df_json = df.to_json()
    return df_json


def data_transformation(**context):
    df_json = context['ti'].xcom_pull(task_ids = "read_data")
    df = pd.read_json(df_json)
    df = df.rename(columns={"Name": "name"})
    df['profit'] = df['close'] - df['open']
    transformed_json = df.to_json()
    return transformed_json
    

default_args = {
    "owner": "Nishan",
    "retries": 3,
    "retry_delay": timedelta(seconds=2)
}

pg_hook = PostgresHook(postgres_conn_id="postgres_connection")

@dag(dag_id="stock_etl_data", 
     start_date= datetime(2023,2,23), 
     schedule_interval="@daily", 
     default_args=default_args, 
     catchup=False)
def stock_etl_pipeline():

    # TASK:1 Read the csv file as a dataframe:
    read_data = PythonOperator(
        task_id = "read_data",
        python_callable = read_dataset

    )

    # TASK:2 Make the changes in the dataframe:
    transform_data = PythonOperator(
        task_id = "transform_data",
        python_callable= data_transformation,
        provide_context = True
    )

    
    # TASK: 3 Create the table:

    @task
    def create_table():

        sql_drop = """
        DROP TABLE IF EXISTS market
        """
        sql_create_table = """
            CREATE TABLE IF NOT EXISTS market (
            date TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INTEGER,
            name VARCHAR(255),
            profit VARCHAR(255)
            )
        """
        pg_hook.run(sql_drop)
        pg_hook.run(sql_create_table)
        
        print("Table Creation market SUCCESS !!!")


    # TASK: 4 Load the data into the table:
    @task()
    def load_data(json_transformed_data):
        df = pd.read_json(json_transformed_data)
        columns = ','.join(df.columns)
        values = ','.join([f"""('{str(row.date)}', {row.open}, {row.high}, {row.low}, {row.close},{row.volume}, '{str(row.name)}', {(row.profit)})"""  for row in df.itertuples()])
        
      
        sql =f"""
            INSERT INTO market ({columns}) VALUES {values};
        """
       
        pg_hook.run(sql)
        print(f"{len(df)} --> records inserted into the table market.")


    read_data >> transform_data >> create_table() >> load_data(transform_data.output)
    
    

stock_pipeline = stock_etl_pipeline()
