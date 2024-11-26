# opb
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
    conn = sqlite3.connect(db_name)
    df.to_sql('users', conn, if_exists='replace', index=False)
    conn.close()
    print("Data Loaded Successfully into Database")

if __name__ == "__main__":
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)
