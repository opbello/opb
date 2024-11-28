import pandas as pd
import sqlite3

# Step 1: Extract
def extract_data():
    # Mock data source
    data = {
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'city': ['New York', 'Los Angeles', 'Chicago']
    }
    df = pd.DataFrame(data)
    print("Data Extracted Successfully")
    return df

# Step 2: Transform
def transform_data(df):
    # Adding a new column for age group
    df['age_group'] = pd.cut(df['age'], bins=[0, 18, 35, 65], labels=['Child', 'Young Adult', 'Adult'])
    print("Data Transformed Successfully")
    return df

# Step 3: Load
def load_data(df, db_name="etl_pipeline.db"):
    # Connecting to the SQLite database
    conn = sqlite3.connect(db_name)
    # Load the DataFrame to the SQLite database, replacing the 'users' table if it exists
    df.to_sql('users', conn, if_exists='replace', index=False)
    conn.close()
    print("Data Loaded Successfully into Database")

# Main Execution
def run_etl_pipeline():
    # Extract
    df = extract_data()
    
    # Transform
    df = transform_data(df)
    
    # Load
    load_data(df)

# Running the ETL pipeline
run_etl_pipeline()
