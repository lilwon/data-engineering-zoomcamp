from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
	"""
		Download trip data from Google Cloud Storage 
	"""
	gcs_path = f"../../data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
	gcs_block = GcsBucket.load("de-zoomcamp-gcs")
	gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
	return Path(f"data/")


@task()
def transform(path: Path) -> pd.DataFrame:
	"""
		Data cleaning example
	"""
	df = pd.read_parquet(path) 
	#print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
	#df['passenger_count'].fillna(0, inplace=True) 
	#print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
	return df


@task()
def write_bq(df: pd.DataFrame) -> None:
	"""
		Write DataFrame to BigQuery
	"""

	gcp_cred_block = GcpCredentials.load("de-zoomcamp-gcp-cred")

	df.to_gbq(
		destination_table="trips_data_all.rides",
		project_id="dtc-de-376220",
		credentials=gcp_cred_block.get_credentials_from_service_account(),
		chunksize=500_000,
		if_exists="append",
	)


@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str) -> int:
	"""
		Main ETL flow to load data into Big Query
	"""
	path = extract_from_gcs(color, year, month)
	df = transform(path)
	write_bq(df)
	print(f"Log: Number of rows in data frame - {len(df)}")
	return len(df)


@flow(log_prints=True) 
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2019, color: str = "yellow"):
	total_rows = 0
	for month in months:
		total_rows += etl_gcs_to_bq(year, month, color)

	print(f"Total rows inserted to BigQuery: {total_rows}")


if __name__ == "__main__":
	color = "yellow"
	months = list(range(1,13))
	year = 2019
	etl_parent_flow(months, year, color)