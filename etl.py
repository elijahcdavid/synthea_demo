import argparse
import data_extract_helper as deh
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

if __name__ == '__main__':
    # Get the input file path from command line arguments
    parser = argparse.ArgumentParser(description='Extract Synthea data')
    parser.add_argument('input', type=str, help='Input file path')
    args = parser.parse_args()

    path = Path(args.input)

    # Load data from synthea to dataframes for conversion to sql
    result = deh.load_data_to_df(path)
    print('data load complete')

    # Load environment variables
    load_dotenv()
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')

    # Create a database connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}/{db_name}')

    try:
        with engine.connect() as conn:
            # Load dataframes to SQL tables
            result['patient_df'].to_sql('patient', conn, if_exists='replace', index=False)
            result['encounter_df'].to_sql('encounter', conn, if_exists='replace', index=False)
            result['condition_df'].to_sql('condition', conn, if_exists='replace', index=False)
            result['diagnostic_df'].to_sql('diagnostic_report', conn, if_exists='replace', index=False)
            result['claim_df'].to_sql('claim', conn, if_exists='replace', index=False)
    except Exception as e:
        print(f'Error: {e}')