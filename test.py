import os
from dotenv import load_dotenv


load_dotenv()

db_name = os.getenv("DB_NAME")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")


print(db_pass)



import os
from dotenv import load_dotenv
import streamlit as st
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

load_dotenv()

db_name = os.getenv("DB_NAME")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")


# Database connection
def get_data_from_db():
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host='localhost',
        port='5432'
    )

