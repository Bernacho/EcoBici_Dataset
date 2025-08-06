import os
from google.cloud import storage
import pyarrow.fs as fs
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcs-key.json'

BUCKET_NAME = 'bernacho-ecobici-datahub'
GCS_ROOT = 'partitioned_historical_data'

def file_exists_in_gcs(file_name):
    year,month = file_name.split("-")
    year = int(year)
    month = int(month)
    filters = [("year","=",year),("month","=",month)]

    print("credentials: ",os.path.exists('gcs-key.json'))

    _fs = fs.GcsFileSystem()
    base_path = f"{BUCKET_NAME}/{GCS_ROOT}/"
    
    df = pd.read_parquet(base_path,columns=['file'], filters=filters, filesystem=_fs, engine='pyarrow').drop_duplicates()
    
    if df.empty:
        return False
    else:
        files = pd.Series(df['file'].unique()).apply(lambda x: x.split("/")[-1] ).tolist()
        if file_name+".csv" in files:
            return True
        else:
            return False



def upload_partitioned_dataset_skip_existing(local_root):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    for root, dirs, files in os.walk(local_root):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_root)
            gcs_path = os.path.join(GCS_ROOT, relative_path).replace("\\", "/")

            blob = bucket.blob(gcs_path)

            if blob.exists():
                print(f"⏩ Skipped existing file: gs://{BUCKET_NAME}/{gcs_path}")
            else:
                # blob.upload_from_filename(local_path)
                print(f"✅ Uploaded {local_path} to gs://{BUCKET_NAME}/{gcs_path}")
