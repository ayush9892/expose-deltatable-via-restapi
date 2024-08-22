import logging
import azure.functions as func
from deltalake import DeltaTable
import duckdb
import pyodbc
import struct
from azure.identity import ClientSecretCredential

import time

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    start_time = time.perf_counter()
    # Don't store credentials in code in PRD. Instead, use Managed Identity to authenticate to storage.
    TENANT_ID = "84c31ca0-ac3b-4eae-ad11-519d80233e6f"
    # Your Service Principal App ID
    CLIENT_ID = "790e7097-4f36-4969-b128-9e3933fc7fb7" # c1dfact-d-spn
    # Your Service Principal Password
    CLIENT_SECRET = "tpU8Q~Wju3TBk0QMgYIldEp.zwRe.p-gylEkTbx3"
    sql_query = req.params.get('sql_query')

    storage_options={'tenant_id': TENANT_ID, 'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET }
    dt = DeltaTable("abfss://presentation@synapselearningadls.dfs.core.windows.net/<<delta folder name>>", storage_options=storage_options)
    parquet_read_options = {
        'coerce_int96_timestamp_unit': 'ms',  # Coerce int96 timestamps to a particular unit
    }
    pyarrow_dataset = dt.to_pyarrow_dataset(parquet_read_options=parquet_read_options)
    silver_fact_sale = duckdb.arrow(pyarrow_dataset)
    results = duckdb.query(sql_query).fetchall()

    end_time = time.perf_counter()
    execution_time = end_time - start_time
    # Check

    return func.HttpResponse(str(execution_time) + "/n" + f"{results}")