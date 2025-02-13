import psycopg2

# Database connection parameters
DB_NAME = "ecom_analysis"
DB_USER = "postgres"
DB_PASSWORD = "123123"
DB_HOST = "localhost"
DB_PORT = "5432"

def create_table():
    """
    Create the ecom_data table in PostgreSQL.
    """
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cursor = conn.cursor()

        # Create table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ecom_data (
            id SERIAL PRIMARY KEY,
            product_id INT,
            customer_id INT,
            sales_amount FLOAT,
            transaction_date DATE
        );
        """)
        
        # Commit the changes
        conn.commit()
        
        print("✅ Table created successfully!")

    except psycopg2.Error as e:
        print(f"❌ Error: {e}")
    
    finally:
        # Close the connection
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_table()
