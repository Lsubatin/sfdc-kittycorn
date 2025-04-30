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

from pathlib import Path
from google.cloud import bigquery
import pyarrow.parquet as pq

def export_dataset_to_parquet(project_id, dataset_id, location, destination_path):
    """
    Exports a BigQuery dataset to Parquet files.

    Args:
        project_id: The ID of your Google Cloud project.
        dataset_id: The ID of the dataset to export.
        location: BigQuery location.
        destination_path: The local path where Parquet files will be saved.
            Should follow the format: /path/to/directory/{table_name}.parquet"
    """

    client = bigquery.Client(project=project_id, location=location)
    dataset_ref = client.get_dataset(dataset_id)

    for table_ref in client.list_tables(dataset_ref):
        table_id = table_ref.table_id
        table_path = destination_path.format(table_name=table_id)  # Format the path with the table name

        print(f"Exporting table {table_id} to {table_path}...")

        rows = client.list_rows(table_ref,page_size=100000).to_arrow(create_bqstorage_client=True) # Adjust page_size as needed

        table_file = Path(table_path)
        table_file.parent.mkdir(parents=True, exist_ok=True) # Create path
        table_file.unlink(missing_ok=True)  # Remove existing file if it exists
        with pq.ParquetWriter(table_path, rows.schema) as writer:  # Open a Parquet writer
            for batch in rows.to_batches():  # Iterate through batches of rows (memory efficient)
               writer.write_batch(batch)

        print(f"Table {table_id} exported successfully.")



PROJECT_ID = ""  # Replace with your project ID
DATASET_NAME = ""  # Replace with your dataset name
LOCATION = "US"
destination_path = "./sample-data/{table_name}.parquet"  # Or local path

if not PROJECT_ID or not DATASET_NAME:
    raise ValueError("Set `PROJECT_ID` and `DATASET_NAME` variables.")

export_dataset_to_parquet(PROJECT_ID, DATASET_NAME, LOCATION, destination_path)