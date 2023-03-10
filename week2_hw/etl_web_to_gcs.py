from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.filesystems import GitHub

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
	"""
		Read data from web into pandas DataFrame 
	"""
	df = pd.read_csv(dataset_url)
	return df


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
	"""
		Fix datatype issues
	"""
	df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
	df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
	print(df.head(2))
	print(f"columns: {df.dtypes}")
	print(f"rows: {len(df)}")
	return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
	"""
		Write DataFrame out as a parquet file 
	"""
	path = Path(f"data/{color}/{dataset_file}.parquet")
	df.to_parquet(path, compression="gzip")
	return path


@task()
def write_gcs(path: Path()) -> None:
	"""
		Uploading local parquet file to Google Cloud Storage
	"""
	gcp_block = GcsBucket.load("de-zoomcamp-gcs")
	gcp_block.upload_from_path(from_path=f"{path}", to_path=path)
	return


@task()
def write_gh(path: Path()) -> None:
	"""
		Write to Github instead?
	"""
	github_block = GitHub.load("github-zoomcamp")
	github_block.upload_from_path(from_path=f"{path}", to_path=path)
	return 	


@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
	"""
		Main ETL function
	"""
	dataset_file = f"{color}_tripdata_{year}-{month:02}"
	dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

	df = fetch(dataset_url) 
	df_clean = clean(df)
	path = write_local(df_clean, color, dataset_file)
	#write_gcs(path) 
	write_gh(path)

	print(f"Log: Sent to gh")


@flow(log_prints=True)
def etl_github_flow(months: list[int] = [1,2], year: int = 2019, color: str = "yellow"):
	for month in months:
		etl_web_to_gcs(year, month, color)
	print("Finished with job")


if __name__ == "__main__": 
	etl_web_to_gcs() 