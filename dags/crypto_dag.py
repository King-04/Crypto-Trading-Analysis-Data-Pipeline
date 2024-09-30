import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2

load_dotenv()

db_name = os.getenv("DB_NAME")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")


# Function to fetch BTC data from Binance API and save to CSV
def fetch_btc_data():
    url = 'https://api.binance.com/api/v3/ticker/24hr'
    params = {'symbol': 'BTCUSDT'}
    response = requests.get(url, params=params)
    data = response.json()

    btc_data = {
        'symbol': data['symbol'],
        'price_change': data['priceChange'],
        'price_change_percent': data['priceChangePercent'],
        'weighted_avg_price': data['weightedAvgPrice'],
        'last_price': data['lastPrice'],
        'volume': data['volume'],
        'quote_volume': data['quoteVolume'],
        'open_time': pd.to_datetime(data['openTime'], unit='ms'),
        'close_time': pd.to_datetime(data['closeTime'], unit='ms')
    }

    df = pd.DataFrame([btc_data])
    df.to_csv('dags/btc_data.csv', index=False)
    print("BTC data saved to CSV.")


# Function to load CSV data into PostgreSQL
def load_data_to_postgres():
    # Read the data from CSV
    df = pd.read_csv('dags/btc_data.csv')

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host='db',  # Docker service name
        port='5432'
    )
    cur = conn.cursor()

    # Insert each row into the btc_data table
    for index, row in df.iterrows():
        cur.execute("""
            INSERT INTO btc_data (symbol, price_change, price_change_percent, weighted_avg_price, last_price, volume, quote_volume, open_time, close_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (row['symbol'], row['price_change'], row['price_change_percent'], row['weighted_avg_price'],
              row['last_price'], row['volume'], row['quote_volume'], row['open_time'], row['close_time']))

    conn.commit()
    cur.close()
    conn.close()
    print("Data loaded into PostgreSQL.")


# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 29),
    'retries': 1
}

with DAG('btc_data_pipeline',
         default_args=default_args,
         schedule_interval='@hourly',  # Fetch data every hour
         catchup=False) as dag:
    # Task to fetch BTC data from Binance
    fetch_data = PythonOperator(
        task_id='fetch_btc_data',
        python_callable=fetch_btc_data
    )

    # Task to load CSV data into PostgreSQL
    load_data = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres
    )

    # Define task dependencies
    fetch_data >> load_data

print(db_name)