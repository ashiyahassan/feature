import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition

# Configure logging for better local output
logging.basicConfig(level=logging.INFO)

# Define a custom DoFn for the transformation step
# This DoFn cleans and transforms a single row (which is a dictionary)
class AnalyzeTaxiTrip(beam.DoFn):
    """
    Cleans and transforms a BigQuery row (dictionary) from the Chicago Taxi Trips public dataset.
    - Calculates trip_miles_per_second.
    - Filters out trips where trip_seconds or trip_miles is zero or null.
    - Selects a subset of relevant columns.
    """
    def process(self, element):
        # BigQuery IO reads the rows as dictionaries.
        
        # Safely retrieve and check for valid data to avoid division by zero and errors
        try:
            trip_seconds = int(element.get('trip_seconds') or 0)
            trip_miles = float(element.get('trip_miles') or 0.0)
            
            # Skip bad records
            if trip_seconds == 0 or trip_miles == 0.0:
                return

            # 1. Transformation: Calculate miles per second
            miles_per_second = trip_miles / trip_seconds

            # 2. Extract and Select: Prepare the final row structure
            output_row = {
                'unique_key': element.get('unique_key'),
                'trip_start_timestamp': element.get('trip_start_timestamp'),
                'trip_end_timestamp': element.get('trip_end_timestamp'),
                'fare': element.get('fare'),
                'trip_miles_per_second': round(miles_per_second, 4), # Round to 4 decimal places
                'payment_type': element.get('payment_type')
            }
            yield output_row
        except (ValueError, TypeError) as e:
            # Log and skip rows that have corrupted data types
            logging.warning(f"Skipping row due to data error: {e}, row: {element}")
            return

def run(argv=None):
    """
    Main function to define and run the Apache Beam pipeline.
    """

    # ----------------------------------------------------------------------
    # ⚠️ 1. Configuration: REPLACE THESE PLACEHOLDERS 
    # ----------------------------------------------------------------------
    # You must create this dataset in your GCP project beforehand.
    PROJECT_ID = 'sage-artifact-464909-c3' 
    GCS_TEMP_LOCATION = 'gs://temp_dataflow_loc/temp' 
    INPUT_TABLE = 'bigquery-public-data.chicago_taxi_trips.taxi_trips'
    # The output table for local testing (ensure the dataset exists)
    OUTPUT_TABLE = f'{PROJECT_ID}:taxi_warehouse.trips_transformed_test' 
    
    # Define runtime options for LOCAL RUNNING
    pipeline_args = [
        f'--project={PROJECT_ID}',
        '--runner=DirectRunner', # <<< Uses your local machine's resources
        '--save_main_session',
        f'--temp_location={GCS_TEMP_LOCATION}',   # Needed to serialize DoFns and imports
        # You can add --temp_location=gs://your-gcs-bucket/temp if needed for BigQuery staging, 
        # but DirectRunner often defaults to local temp files first.
    ]
    # ----------------------------------------------------------------------


    # Parse the arguments and create PipelineOptions
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = False # This is a batch job

    # Define the schema for the output BigQuery table
    OUTPUT_TABLE_SCHEMA = {
        'fields': [
            {'name': 'unique_key', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'trip_start_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'trip_end_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'fare', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'trip_miles_per_second', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'payment_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }
    
    # ----------------------------------------------------------------------
    # 2. Pipeline Definition 
    # ----------------------------------------------------------------------
    INPUT_T = 'bigquery-public-data.chicago_taxi_trips.taxi_trips'
    with beam.Pipeline(options=options) as p:
        # A. Read from BigQuery (Extraction)
        read_data = (
            p 
            | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
                # Limiting the data to 10,000 rows for quick local testing
                query=f'SELECT * FROM `{INPUT_T}` WHERE trip_start_timestamp >= "2019-09-05 07:15:00 UTC" LIMIT 10000',
                use_standard_sql=True
            )
        )
        
        # B. Apply Transformation (ParDo)
        transformed_data = (
            read_data 
            | 'AnalyzeAndTransform' >> beam.ParDo(AnalyzeTaxiTrip())
        )
        
        # C. Write to BigQuery (Load)
        transformed_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=OUTPUT_TABLE,
            schema=OUTPUT_TABLE_SCHEMA,
            # Create the table if it doesn't exist
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            # Overwrite the table on each run (useful for testing)
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE
        )

if __name__ == '__main__':
    run()