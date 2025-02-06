import pandas as pd
from sqlalchemy import create_engine

# Load the cleaned data
df = pd.read_csv('data/processed_data/telegram_cleaned_data.csv')

# Create a connection to PostgreSQL
engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')

# Load the data into PostgreSQL
df.to_sql('telegram_messages', engine, index=False, if_exists='replace')
print("Data loaded into PostgreSQL successfully!")

