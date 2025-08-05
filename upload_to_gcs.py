import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcs-key.json'

def upload_partitioned_dataset_skip_existing(local_root, bucket_name, gcs_root):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for root, dirs, files in os.walk(local_root):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_root)
            gcs_path = os.path.join(gcs_root, relative_path).replace("\\", "/")

            blob = bucket.blob(gcs_path)

            if blob.exists():
                print(f"⏩ Skipped existing file: gs://{bucket_name}/{gcs_path}")
            else:
                blob.upload_from_filename(local_path)
                print(f"✅ Uploaded {local_path} to gs://{bucket_name}/{gcs_path}")
