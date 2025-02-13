import pandas as pd

def load_kaggle_data(file_path):
    
    # chcking file extantion and loads
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith('.parquet'):
        df = pd.read_parquet(file_path)
    else:
        raise ValueError("Unsupported file format")
    
    print(f"Data loaded successfully from {file_path}")
    return df

# Specify the path to your dataset
data_path = r"C:\Users\hp\Desktop\python\Ethiopian-Ecom-Analysis\data\BDDB\olist_geolocation_dataset.csv"  # Change this to your actual file path

# Call the function to load the data
df = load_kaggle_data(data_path)

# Optionally, you can check the first few rows of the dataframe
print(df.head())
