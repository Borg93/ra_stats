import dlt
from matomo import matomo_reports, matomo_visits
from datasets import load_dataset, Dataset, Features, Value
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

MATOMO_API_TOKEN = os.getenv('MATOMO_API_TOKEN')
MATOMO_URL = os.getenv('MATOMO_URL')
SITE_ID = int(os.getenv('SITE_ID'))
HF_TOKEN = os.getenv('HF_TOKEN')
REPO_NAME = "Gabriel/matomo_stats"  

@dlt.destination(batch_size=0, loader_file_format="parquet")
def local_destination(items, table):
    print(f"\nProcessing table: {table}")
    df = pd.read_parquet(items)
    print(df.head()) 
    return items 

@dlt.destination(batch_size=0, loader_file_format="parquet")
def hf_destination(items, table):

    existing_dataset = load_dataset(REPO_NAME)
    existing_df = existing_dataset["train"].to_pandas()
    existing_df = None

    new_df = pd.read_parquet(items)

    if existing_df is not None:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    features = Features({
        "resource_name": Value("string"),
        "timestamp": Value("timestamp[s]"),
        "data": Value("string"), 
        "_dlt_load_id": Value("string"),
        "_dlt_id": Value("string")
    })

    combined_dataset = Dataset.from_pandas(combined_df, features=features)

    combined_dataset.push_to_hub(
        REPO_NAME,
        private=False,
        token=HF_TOKEN
    )

@dlt.resource(table_name="matomo_reports")
def load_reports():
    reports = matomo_reports(
        api_token=MATOMO_API_TOKEN,
        url=MATOMO_URL,
        site_id=SITE_ID
    )
    for report in reports:
        yield {
            "resource_name": report["resource_name"],
            "timestamp": datetime.now().replace(microsecond=0).isoformat(),
            "data": str(report)  
        }

@dlt.resource(table_name="matomo_visits")
def load_visits():
    visits = matomo_visits(
        api_token=MATOMO_API_TOKEN,
        url=MATOMO_URL,
        live_events_site_id=SITE_ID
    )
    for visit in visits:
        yield {
            "resource_name": "visits",
            "timestamp": datetime.now().replace(microsecond=0).isoformat(),
            "data": str(visit) 
        }

pipeline = dlt.pipeline(
    pipeline_name="matomo_stats_pipeline",
    destination=local_destination
)

print("Loading reports...")
load_info_reports = pipeline.run(load_reports())
print("Reports Load Info:", load_info_reports)

print("Loading visits...")
load_info_visits = pipeline.run(load_visits())
print("Visits Load Info:", load_info_visits)
