import datetime
import json
import os

import pandas as pd
import requests
from prefect import task
from utils import log
from pathlib import Path
import time



@task
def get_api_data(url: str, local_path: str) -> Path:
    
    all_data = []
    for i in range(3):
        try:
            log(f"Downloading data: {i}")
            response = requests.get(url)
            response.raise_for_status()
            json_data = response.json()
            all_data.extend(json_data)
            time.sleep(2)

        except requests.exceptions.RequestException as error:
            log(f"Error: {error}")
            raise error
    log('Data extracted with success!')

    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    
    with local_path.open('w') as f:
        json.dump(all_data, f)
        log(f"Local path: {local_path}")

    return local_path

@task
def process_data(local_path: Path) -> pd.DataFrame:

    try:
        with local_path.open('r') as f:
            data = json.load(f)
        log("JSON data successfully loaded into a dictionary.")

        dataframe = pd.DataFrame(data)
        log("Data has been converted to a DataFrame.")
        log(f'DataFrame head: \n{dataframe.head()}')

        return dataframe

    except json.JSONDecodeError as e:
        log(f"Failed to decode JSON data: {e}")
        raise e

@task
def save_data_to_parquet(dataframe: pd.DataFrame, base_path: str) -> None:

    if 'brewery_type' not in dataframe.columns or 'state_province' not in dataframe.columns:
        log("Dataframe must contain 'brewery_type' and 'state_province' columns for partitioning")
        raise ValueError("Dataframe must contain 'brewery_type' and 'state_province' columns for partitioning")

    base_path = Path(base_path)
    base_path.mkdir(parents=True, exist_ok=True)

    log("Saving data to Parquet files partitioned by 'brewery_type' and 'state_province'...")
    dataframe.to_parquet(base_path, partition_cols=['brewery_type', 'state_province'], engine='pyarrow', index=False)
    log(f"Data saved successfully as Parquet at {base_path}")