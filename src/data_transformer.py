import pandas as pd

# Load scraped data
df = pd.read_csv('data/raw_data/telegram_data.csv')

# Data cleaning (e.g., handle missing values, convert timestamps)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['text'] = df['text'].fillna('No Text')

# You can add additional cleaning steps like removing duplicates or formatting data here

# Save the cleaned data
df.to_csv('data/processed_data/telegram_cleaned_data.csv', index=False)
print("Data cleaned and saved successfully!")
