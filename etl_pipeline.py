import pandas as pd
import sqlite3
import logging

# Configure logging
logging.basicConfig(filename="etl_log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")

def extract(file_path):
    logging.info("Extracting data from CSV file.")
    return pd.read_csv(file_path)

def transform(data):
    logging.info("Transforming data: Filling missing values and formatting dates.")
    data["Treatment_Cost"] = data["Treatment_Cost"].fillna(0)  # Fill missing values
    data["Visit_Date"] = pd.to_datetime(data["Visit_Date"])  # Convert to datetime
    return data

def load(data, db_path):
    logging.info("Loading data into SQLite database.")
    conn = sqlite3.connect(db_path)
    data.to_sql("patient_data", conn, if_exists="replace", index=False)
    conn.close()
    logging.info("Data successfully loaded into the database.")

if __name__ == "__main__":
    logging.info("ETL process started.")
    
    # Step 1: Extract
    raw_data = extract("input_data.csv")
    
    # Step 2: Transform
    transformed_data = transform(raw_data)
    
    # Step 3: Load
    load(transformed_data, "etl_output.db")
    
    logging.info("ETL process completed successfully.")

