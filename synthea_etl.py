import argparse
import synthea_helpers as sh
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract Synthea data')
    parser.add_argument('input', type=str, help='Input file path')
    args = parser.parse_args()

    path = Path(args.input)

    result = sh.load_data_to_df(path)