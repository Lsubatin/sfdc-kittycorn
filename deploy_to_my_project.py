# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import bigquery
from io import FileIO
import os

def import_parquet_files_to_bigquery(project_id, dataset_id, location, parquet_folder):
    """
    Imports Parquet files from a folder into BigQuery, creating a table for each file.

    Args:
        project_id: The ID of your Google Cloud project.
        dataset_id: The ID of the dataset to import into.
        location: BigQuery location.
        parquet_folder: The local path of the folder containing Parquet files.
    """

    client = bigquery.Client(project=project_id, location=location)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    dataset_ref = client.create_dataset(dataset, exists_ok=True)

    parquet_folder = os.path.abspath(parquet_folder)
    for filename in os.listdir(parquet_folder):
        if filename.endswith(".parquet"):
            table_id = filename[:-8]  # Remove ".parquet" extension
            table_ref = dataset_ref.table(table_id)

            # Check if table already exists, if yes, you might want to append,
            # replace, or skip - handle according to your needs.
            try:
                client.get_table(table_ref) # throws exception if doesn't exist
                print(f"Table {table_id} already exists. Skipping.")  # Or choose to append/replace
                continue
            except:
                pass # handle exception to continue processing other files

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                autodetect=True,  # Auto-detect schema
                write_disposition = 'WRITE_TRUNCATE'  # WRITE_APPEND, WRITE_EMPTY
            )

            with FileIO(os.path.join(parquet_folder, filename)) as f:
                load_job = client.load_table_from_file(
                    f,
                    table_ref,
                    job_config=job_config,
                    location=location
                )
                load_job.result()  # Waits for the job to complete.
                print(f"Loaded {load_job.output_rows} rows into {table_ref.path}")


PROJECT_ID = ""  # Replace with your project ID
DATASET_NAME = ""  # Replace with your dataset name
LOCATION = "US"
parquet_folder = "./sample-data"

if not PROJECT_ID or not DATASET_NAME:
    raise ValueError("Set `PROJECT_ID` and `DATASET_ID` variables.")

import_parquet_files_to_bigquery(PROJECT_ID, DATASET_NAME, LOCATION, parquet_folder)