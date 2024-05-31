from prefect import Flow, Parameter
from tasks import get_api_data, process_data, save_data_to_parquet, save_data_to_csv, run_dbt, run_dbt_seed
from pathlib import Path

with Flow('Extract brewery data') as brew_flow:

    # Parameters
    url = Parameter('url', default='https://api.openbrewerydb.org/v1/breweries')
    local_file_path = Parameter('local_path', default="data/raw_data.json")
    base_path = Parameter('base_path', default='transformed_data/')
    csv_path = Parameter('csv_path', default='gold/seeds/raw_data.csv')

    # Tasks
    local_path = get_api_data(url=url, local_path=local_file_path)
    dataframed_data = process_data(local_path=local_path)
    parquet = save_data_to_parquet(dataframe=dataframed_data, base_path=base_path)
    csv = save_data_to_csv(dataframe=dataframed_data, csv_path=csv_path)
    seed = run_dbt_seed(upstream_task=csv)
    run_dbt(upstream_task=seed)