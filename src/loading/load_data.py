import pandas as pd
import psycopg2

# Database connection parameters
DB_NAME = "ecom_analysis"
DB_USER = "postgres"
DB_PASSWORD = "123123"
DB_HOST = "localhost"
DB_PORT = "5432"

def insert_data(csv_file_path):
    """
    Insert cleaned data into PostgreSQL database.
    """
    try:
        # Load data from CSV into pandas DataFrame
        df = pd.read_csv(csv_file_path)

        # Print the columns to ensure the data is loaded correctly
        print(f"Columns in the CSV: {df.columns.tolist()}")

        # Convert 'customer_zip_code_prefix' to string if it contains alphanumeric characters
        df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str)

        # Ensure other relevant columns are also in the correct format (if necessary)
        df['customer_id'] = df['customer_id'].astype(str)  # Make sure customer_id is also a string
        df['customer_unique_id'] = df['customer_unique_id'].astype(str)  # customer_unique_id should be string

        # Convert 'customer_city' and 'customer_state' to string if necessary
        df['customer_city'] = df['customer_city'].astype(str)
        df['customer_state'] = df['customer_state'].astype(str)

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cursor = conn.cursor()

        # Prepare the insert query with text column types
        insert_query = """
        INSERT INTO ecom_data (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
        VALUES (%s, %s, %s, %s, %s)
        """

        # Convert DataFrame rows into a list of tuples for insertion
        data_to_insert = df[['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']].values.tolist()

        # Insert data into the table
        cursor.executemany(insert_query, data_to_insert)

        # Commit the changes to the database
        conn.commit()

        print("✅ Data inserted successfully into ecom_data table!")

    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        # Close the connection
        if conn:
            cursor.close()
            conn.close()

# Run the function with the correct CSV path
insert_data(r"C:\Users\hp\Desktop\python\Ethiopian-Ecom-Analysis\data\cleaned_dataset.csv")
