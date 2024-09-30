import os
from dotenv import load_dotenv


load_dotenv()

db_name = os.getenv("DB_NAME")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")


print(db_pass)
