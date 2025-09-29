#!/bin/sh
# $1 is the model name (e.g., simple_model)
# $2 is the dbt target/environment (e.g., dev)

# Run dbt debug to confirm connection status
dbt debug --target $2 --profiles-dir .

# Run the selected dbt model
dbt run --select $1 --target $2 --profiles-dir .