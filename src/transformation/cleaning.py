import pandas as pd
import os
#tha path to our dataset
data_path = r"C:\Users\hp\Desktop\python\Ethiopian-Ecom-Analysis\data\BDDB\olist_customers_dataset.csv"

def load_data(file_path):
   
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith('.parquet'):
        df = pd.read_parquet(file_path)
    else:
        raise ValueError("Unsupported file format. Use CSV or Parquet.")

    print(f"Data loaded successfully from {file_path}")
    return df

def clean_data(df):
    """
    Clean the dataset by handling missing values, removing duplicates, and formatting data types.
    """
    # Drop rows with missing values (or use df.fillna() if needed)
    df = df.dropna()

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Convert date column if it exists
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Convert amount column to numeric if it exists
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Create a 'year' column if 'date' exists
    if 'date' in df.columns:
        df['year'] = df['date'].dt.year

    print("Data cleaned successfully")
    return df

def save_cleaned_data(df, output_path):
    """
    Save the cleaned data as a CSV file.
    """
    df.to_csv(output_path, index=False)
    print(f"Cleaned data saved to {output_path}")

if __name__ == "__main__":
    # Load, clean, and save the dataset
    try:
        df = load_data(data_path)
        df_cleaned = clean_data(df)

        # Set output path
        output_path = r"C:\Users\hp\Desktop\python\Ethiopian-Ecom-Analysis\data\cleaned_dataset.csv"
        save_cleaned_data(df_cleaned, output_path)
    
    except Exception as e:
        print(f"Error: {e}")
