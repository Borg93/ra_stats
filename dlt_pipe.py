import dlt
import os
import requests
import yaml
import pandas as pd
from datetime import datetime
from datasets import load_dataset, Dataset, Features, Value
from dotenv import load_dotenv

load_dotenv()

HF_TOKEN = os.getenv('HF_TOKEN')


BASE_URL = "https://huggingface.co/api/{}/{}?expand[]=downloads&expand[]=downloadsAllTime&expand[]=sha"
REPO_NAME = "Gabriel/hf_repo_stats" 

def fetch_repo_stats(repo_category: str, repo: str) -> dict:
    url = BASE_URL.format(repo_category, repo)
    data = requests.get(url).json()
    repo_type = "model" if repo_category == "models" else "dataset"
    timestamp = datetime.now().replace(microsecond=0).isoformat()
    return {
        "repo": repo,
        "type": repo_type,
        "sha": data.get("sha"),
        "downloads": data.get("downloads"),
        "downloads_all_time": data.get("downloadsAllTime"),
        "timestamp": timestamp
    }

@dlt.resource(table_name="hf_repo_stats")
def hf_repo_stats():
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    for repo in config.get("models", []):
        yield fetch_repo_stats("models", repo)
    for repo in config.get("datasets", []):
        yield fetch_repo_stats("datasets", repo)

@dlt.destination(batch_size=0, loader_file_format="parquet")
def hf_destination(items, table):

    existing_dataset = load_dataset(REPO_NAME)
    existing_df = existing_dataset["train"].to_pandas()
    new_df = pd.read_parquet(items)

    if existing_df is not None:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    features = Features({
        "repo": Value("string"),
        "type": Value("string"),
        "downloads": Value("int32"),
        "downloads_all_time": Value("int32"),
        "timestamp": Value("timestamp[s]") ,
        "_dlt_load_id": Value("string"),
        "_dlt_id": Value("string"),
        "sha": Value("string"),
    })

    combined_dataset = Dataset.from_pandas(combined_df, features=features)
    combined_dataset.push_to_hub(
        REPO_NAME,
        private=False, 
        token=HF_TOKEN
    )

pipeline = dlt.pipeline(pipeline_name="hf_repo_stats_pipeline", destination=hf_destination)
load_info = pipeline.run(hf_repo_stats())
