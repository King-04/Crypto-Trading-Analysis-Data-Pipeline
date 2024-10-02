import os
from dotenv import load_dotenv
import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt

load_dotenv()

db_name = os.getenv("DB_NAME")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")


# Database connection function
def get_data_from_db():
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host='localhost',  # Adjust host if necessary
        port='5432'
    )
    query = """
    SELECT symbol, price, volume_24h, retrieval_time 
    FROM crypto_data 
    WHERE price > 0
    ORDER BY retrieval_time DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# Streamlit app title
st.title("Crypto Market Data Visualization")

# Fetch the data from PostgreSQL
df = get_data_from_db()

# Get the most recent data for each symbol
latest_data = df.groupby('symbol').first().reset_index()

# Sidebar for user selection
symbols = latest_data['symbol'].unique()
top_n = st.sidebar.slider("Select the number of top coins by volume to display", min_value=5, max_value=50, value=10)

# Display top `n` coins with the highest trading volume
st.subheader(f"Top {top_n} Cryptocurrencies by 24h Volume")
top_volume = latest_data.nlargest(top_n, 'volume_24h')

# Plotting a bar chart for the top `n` coins by trading volume
fig = px.bar(top_volume, x='symbol', y='volume_24h', title=f"Top {top_n} Cryptocurrencies by 24h Volume")
st.plotly_chart(fig)

# Plotting a pie chart to show distribution of volume among top coins
st.subheader("Volume Distribution Among Top Coins")
fig_pie = px.pie(top_volume, names='symbol', values='volume_24h', title=f"Volume Distribution Among Top {top_n} Coins")
st.plotly_chart(fig_pie)

# Display raw data (optional)
if st.checkbox("Show Raw Data"):
    st.write(latest_data)

# Compare price and volume for user-selected coin
st.subheader("Compare Price and Volume for Selected Coins")
selected_symbols = st.multiselect("Select coins to compare", symbols, default=symbols[:3])

if selected_symbols:
    filtered_data = df[df['symbol'].isin(selected_symbols)]

    # Line chart of price over time for selected coins
    st.subheader("Price Trend Over Time")
    fig_price = px.line(filtered_data, x='retrieval_time', y='price', color='symbol', title="Price Over Time")
    st.plotly_chart(fig_price)

    # Line chart of volume over time for selected coins
    st.subheader("Volume Trend Over Time")
    fig_volume = px.line(filtered_data, x='retrieval_time', y='volume_24h', color='symbol', title="Volume Over Time")
    st.plotly_chart(fig_volume)
