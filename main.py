# main.py
import functions_framework
from google.cloud import bigquery
import requests
import os
import json

# Replace with your BigQuery project and dataset info
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
DATASET_ID = "crypto_data"
TABLE_ID = "prices"

# Public API endpoint for cryptocurrency data
API_URL = "https://api.coingecko.com/api/v3/simple/price"
# You can add more currencies here
PARAMS = {
    "ids": "bitcoin,ethereum,cardano",
    "vs_currencies": "usd"
}

@functions_framework.http
def fetch_and_load_crypto_data(request):
    """
    HTTP Cloud Function that fetches crypto data and loads it into BigQuery.
    This function is triggered by a Cloud Workflow.
    """
    try:
        print("Starting data fetch from API...")
        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        print(f"Successfully fetched data: {data}")

        # BigQuery client setup
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(DATASET_ID, project=PROJECT_ID)
        table_ref = dataset_ref.table(TABLE_ID)

        # Prepare data for BigQuery
        rows_to_insert = []
        for currency, value in data.items():
            rows_to_insert.append({
                "currency": currency,
                "usd_price": value.get("usd"),
            })

        print(f"Inserting {len(rows_to_insert)} rows into BigQuery...")
        errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            print("Encountered errors while inserting rows: {}".format(errors))
            return json.dumps({"status": "Error", "message": str(errors)}), 500
        else:
            print(f"Data successfully loaded into {TABLE_ID}.")
            return json.dumps({"status": "Success", "message": f"Loaded {len(rows_to_insert)} rows"}), 200

    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return json.dumps({"status": "Error", "message": f"API request failed: {e}"}), 500
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return json.dumps({"status": "Error", "message": f"An unexpected error occurred: {e}"}), 500
