# Serverless ETL 

Serverless ETL for FAANG stocks from Bloomberg API to BigQuery.

Data source: [RapidAPI Bloomberg](https://rapidapi.com/apidojo/api/bloomberg-market-and-financial-news/)


## How to Run (local)

1. Build a virtual environment.

    ```bash
    python -m venv venv
    source venv/bin/activate
    ```

1. Install the dependencies.

    ```bash
    pip install -r requirements.txt
    ```

1. Initiate environment variables in `.env` file.

    e.g.:

    ```bash
    export RAPIDAPI_API_KEY=Your_RapidAPI_key
    export BQ_PROJECT_DATASET_TABLE=Your_BQ_table_id
    echo -e \
    "RAPIDAPI_API_KEY=\"$RAPIDAPI_API_KEY\"\nBQ_PROJECT_DATASET_TABLE=\"$BQ_PROJECT_DATASET_TABLE\"" \
    > .env
    ```

    Look at `.env_sample` as environment file reference.

1. Run the application

    ```bash
    python app.py
    ```

    Example output:
    ```
    (venv)$ python app.py
    Invoke
    Extracting data from bloomberg api...
    Transforming data from bloomberg api to dataframe...
    6 new record(s) are found
    New rows have been added.
    6 record(s) inserted to BigQuery Table

    (venv)$ 
    ```

1. Check the BigQuery table and ensure the data successfully loaded.


## How to Run (Serverlessly)

Refer to this [Medium article](https://medium.com/google-cloud-indonesia/scheduling-streaming-insert-ke-bigquery-menggunakan-serverless-option-3e54b4825a22) to deploy it on GCP.