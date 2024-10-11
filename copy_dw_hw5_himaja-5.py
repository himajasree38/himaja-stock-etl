#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}


with DAG(
    'stock_etl_dag',
    default_args=default_args,
    description='ETL DAG for Stock Data using Alpha Vantage and Snowflake',
    schedule_interval=None,  
    start_date=datetime(2024, 10, 10),
    catchup=False,
) as dag:

    
    symbol = "IBM"
    vantage_api_key = Variable.get('vantage_api_key', default_var='default_value_here')
    
    if vantage_api_key == 'default_value_here':
        print("Warning: Variable 'vantage_api_key' is not found, using default value.")

    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}"
    
    
    def return_snowflake_conn():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        return conn.cursor()

    @task
    def extract(url):
        response = requests.get(url)
        data = response.json()
        return data

    @task
    def process_stock_data(data):
        results = []
        for date, stock_info in data.get("Time Series (Daily)", {}).items():
            stock_info["date"] = date
            stock_info["symbol"] = "IBM"
            results.append(stock_info)
        return results[:90]

    @task
    def load_data_to_snowflake(results):
        cursor = return_snowflake_conn()
        table = "stock_db.raw_data.stock_prices"

        try:
            cursor.execute("BEGIN;")
            create_table_query = f"""
            CREATE OR REPLACE TABLE {table} (
                "open" FLOAT,
                "high" FLOAT,
                "low" FLOAT,
                "close" FLOAT,
                "volume" INT,
                "date" DATE,
                "symbol" VARCHAR(20),
                PRIMARY KEY ("date", "symbol")
            );
            """
            cursor.execute(create_table_query)

            for r in results:
                insert_sql = f"""
                INSERT INTO {table} ("open", "high", "low", "close", "volume", "date", "symbol")
                VALUES ({r['1. open']}, {r['2. high']}, {r['3. low']}, {r['4. close']}, {r['5. volume']}, '{r['date']}', '{r['symbol']}')
                """
                cursor.execute(insert_sql)

            cursor.execute("COMMIT;")
        except Exception as e:
            cursor.execute("ROLLBACK;")
            raise e

    stock_data = extract(url)
    processed_data = process_stock_data(stock_data)
    load_data_to_snowflake(processed_data)

