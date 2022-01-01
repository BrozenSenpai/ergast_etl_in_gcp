import urllib.request
import os
import zipfile
import re
import shutil

import pandas as pd
from google.cloud import storage
from google.cloud import bigquery


def ergast_bq_etl(request):
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket("f1_bucket")
    
    # extract
    url = "http://ergast.com/downloads/f1db_csv.zip"
    # download zip to temporary directory
    urllib.request.urlretrieve(url, "/tmp/f1db_csv.zip")
    # unzip to temporary subirectory
    with zipfile.ZipFile("/tmp/f1db_csv.zip", "r") as z:
        z.extractall("/tmp/csv")
    os.remove('/tmp/f1db_csv.zip')
    
    # transform
    # replace \N values in results.csv with blank, otherwise bigquery wont be able  to parse it
    df = pd.read_csv("/tmp/csv/results.csv")
    df = df.replace("\\N", "")
    df.to_csv("/tmp/csv/results.csv", index=False)
    
    # storage load
    # load csvs to google storage's bucket
    for file in os.listdir("/tmp/csv"):
        blob = bucket.blob(f"csv/{file}")
        blob.upload_from_filename(f"/tmp/csv/{file}")
    shutil.rmtree('/tmp/csv')
     # bigquery load
     for csv in list(storage_client.list_blobs(bucket)):
         table_id = f"project-123456.f1_ds.f1_{re.split('[/.]', csv.name)[1]}"
         job_config = bigquery.job.LoadJobConfig(
             write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
             autodetect=True,
             source_format=bigquery.SourceFormat.CSV,
         )
         uri = f"gs://f1_bucket/{csv.name}"
         load_job = bq_client.load_table_from_uri(
            uri, table_id, job_config=job_config)
         load_job.result()
         destination_table = bq_client.get_table(table_id)          
    return "OK"
