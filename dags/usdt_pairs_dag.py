import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2

# Load environment variables
load_dotenv()

db_name = os.getenv("DB_NAME")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")


# Function to fetch USDT pairs data and save to CSV (unchanged)
def extract_data():
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    response = requests.get(url)

    if response.status_code == 200:
        exchange_info = response.json()
        symbols = [symbol['symbol'] for symbol in exchange_info['symbols']]
        usdt_pairs = [symbol for symbol in symbols if 'USDT' in symbol]

        ticker_url = 'https://api.binance.com/api/v3/ticker/24hr'
        tickers = requests.get(ticker_url).json()

        data = []
        for pair in usdt_pairs:
            ticker = next((t for t in tickers if t['symbol'] == pair), None)
            if ticker:
                row = {
                    'Timestamp': datetime.now(),
                    'Symbol': pair,
                    'Price': float(ticker['lastPrice']),
                    'Volume_24h': float(ticker['quoteVolume']),
                    'Change_24h': float(ticker['priceChangePercent']),
                    'Bid_Price': float(ticker['bidPrice']),
                    'Ask_Price': float(ticker['askPrice']),
                    'High_24h': float(ticker['highPrice']),
                    'Low_24h': float(ticker['lowPrice']),
                    'Quote_Asset_Volume_24h': float(ticker['quoteVolume']),
                    'Number_of_Trades_24h': float(ticker['count']),
                }
                data.append(row)

        df = pd.DataFrame(data)
        df.to_csv('dags/usdt_pairs_raw.csv', index=False)
    else:
        print(f'Error fetching data: {response.status_code}')


# Function to clean the data
def clean_data():
    # Load raw data from CSV
    df = pd.read_csv('dags/usdt_pairs_raw.csv')

    # Filter out rows where price or relevant fields are zero
    cleaned_df = df[(df['Price'] > 0) & (df['Volume_24h'] > 0)]

    # Save cleaned data to a new CSV
    cleaned_df.to_csv('dags/usdt_pairs_cleaned.csv', index=False)
    print("Cleaned data saved to CSV.")


# Function to load cleaned CSV data into PostgreSQL
def load_data_to_postgres():
    # Read the cleaned data from CSV
    df = pd.read_csv('dags/usdt_pairs_cleaned.csv')

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host='db',  # Docker service name
        port='5432'
    )
    cur = conn.cursor()

    # Insert each row into the crypto_data table
    for index, row in df.iterrows():
        cur.execute("""
            INSERT INTO crypto_data (
                symbol, price, volume_24h, change_24h, bid_price, ask_price, high_24h, low_24h, quote_asset_volume_24h, number_of_trades_24h, retrieval_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            row['Symbol'], row['Price'], row['Volume_24h'], row['Change_24h'], row['Bid_Price'],
            row['Ask_Price'], row['High_24h'], row['Low_24h'], row['Quote_Asset_Volume_24h'],
            row['Number_of_Trades_24h'], row['Timestamp']
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("Cleaned data loaded into PostgreSQL.")


# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 29),
    'retries': 1
}

with DAG('usdt_pairs_data_clean_pipeline',
         default_args=default_args,
         schedule_interval='*/30 * * * *',  # Run every 30 minutes
         catchup=False) as dag:

    # Task to extract raw USDT pairs data
    extract_task = PythonOperator(
        task_id='extract_usdt_pairs_data',
        python_callable=extract_data
    )

    # Task to clean the data
    clean_task = PythonOperator(
        task_id='clean_usdt_pairs_data',
        python_callable=clean_data
    )

    # Task to load cleaned data into PostgreSQL
    load_task = PythonOperator(
        task_id='load_cleaned_data_to_postgres',
        python_callable=load_data_to_postgres
    )

    # Define task dependencies
    extract_task >> clean_task >> load_task
