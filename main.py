import functions_framework
from google.cloud import bigquery
import requests
import json

# BigQuery dataset and table info
DATASET_ID = "crypto_data"
TABLE_ID = "prices"

# Public API for crypto data
API_URL = "https://api.coingecko.com/api/v3/simple/price"
PARAMS = {
    "ids": "bitcoin,ethereum,cardano",
    "vs_currencies": "usd"
}

@functions_framework.http
def fetch_and_load_crypto_data(request):
    try:
        print("Starting data fetch from API...")
        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status()
        data = response.json()
        print(f"Successfully fetched data: {data}")

        # BigQuery client setup
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(TABLE_ID)

        full_table_id = f"{bigquery_client.project}.{DATASET_ID}.{TABLE_ID}"
        print(f"Loading data into table: {full_table_id}")

        # Prepare data
        rows_to_insert = [
            {"currency": currency, "usd_price": value.get("usd")}
            for currency, value in data.items()
        ]

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
