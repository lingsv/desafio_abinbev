import json
import pandas as pd
import requests
from prefect import task
from utils import log
from pathlib import Path
import time
import subprocess


@task
def get_api_data(url: str, local_path: str) -> Path:
    """Extracts data from the API.

    Args:
        url (str): API endpoint.
        local_path (str): Local path to store the extracted data.

    Raises:
        error: Request error when the status is not 200.

    Returns:
        Path:Final path with the data.
    """
    
    all_data = []
    for i in range(8500):
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
    """Process the extracted data into a dataframe 
        and process some columns of the dataframe.

    Args:
        local_path (Path): Path with data.

    Raises:
        error: Returns an error if it fails to parse the data.

    Returns:
        pd.DataFrame: Final dataframe.
    """

    try:
        with local_path.open('r') as f:
            data = json.load(f)
        log("JSON data successfully loaded into a dictionary.")

        dataframe = pd.DataFrame(data)
        log("Data has been converted to a DataFrame.")
        # merging address columns
        log("Processing data...")
        dataframe = dataframe.drop(['address_2', 'address_3'], axis=1)
        dataframe.rename(columns={'address_1': 'address'}, inplace=True)
        # Ensure phone is a string and other numeric fields are floats
        dataframe['phone'] = dataframe['phone'].astype(str)
        dataframe['longitude'] = dataframe['longitude'].astype(float)
        dataframe['latitude'] = dataframe['latitude'].astype(float)
        log(f'DataFrame head: \n{dataframe.head()}')

        return dataframe

    except json.JSONDecodeError as error:
        log(f"Failed to decode JSON data: {error}")
        raise error

@task
def save_data_to_csv(dataframe: pd.DataFrame, csv_path: str) -> None:
    """Saves the raw data in a CSV file stored in the `seeds` folder. 

    Args:
        dataframe (pd.DataFrame): The transformed dataframe.
        csv_path (str): The seeds folder path.
    """
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(csv_path, index=False)
    log(f"Data saved successfully as CSV at {csv_path}")

@task
def save_data_to_parquet(dataframe: pd.DataFrame, base_path: str) -> None:
    """Saves the data in partitioned parquet files.

    Args:
        dataframe (pd.DataFrame): The transformed dataframe.
        base_path (str): The local path that will store the parquet files.

    Raises:
        ValueError: Raise an error if the dataframe does not contain the partition column.
    """
    if 'state' not in dataframe.columns:
        log("Dataframe must contain 'state' columns for partitioning")
        raise ValueError("Dataframe must contain 'state' columns for partitioning")

    base_path = Path(base_path)
    base_path.mkdir(parents=True, exist_ok=True)

    log("Saving data to Parquet files partitioned by 'state'...")
    dataframe.to_parquet(base_path, partition_cols=['state'], engine='pyarrow', index=False)
    log(f"Data saved successfully as Parquet at {base_path}")
    
@task
def run_dbt_seed(upstream_task=save_data_to_csv):
    """Executes the seed operation in dbt to materialize the table later.

    Args:
        upstream_task (_type_, optional): Sets the necessary task that comes before the dbt operation.
         Defaults to save_data_to_csv.

    Raises:
        Exception: If the operation returns a code diferrent from 0, it raises an error.

    Returns:
        _type_: Returns the DBT interface for the process execution.
    """
    result = subprocess.run(["dbt", "seed"], cwd="gold", capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"dbt seed failed: {result.stderr}")
    log(result.stdout)
    return result.stdout


@task
def run_dbt(upstream_task=run_dbt_seed):
    """Executes the materialization of the table and the view in dbt.

    Args:
        upstream_task (_type_, optional): Sets the necessary task that comes before the dbt operation. Defaults to run_dbt_seed.

    Raises:
        Exception: If the operation returns a code diferrent from 0, it raises an error.
    Returns:
        _type_: Returns the DBT interface for the process execution.
    """
    result = subprocess.run(["dbt", "seed"], cwd="gold", capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"dbt seed failed: {result.stderr}")
    log(result.stdout)
    return result.stdout
