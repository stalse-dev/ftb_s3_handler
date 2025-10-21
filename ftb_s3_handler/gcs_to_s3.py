# tendo que estragar meu repo pq sim
import os
import gc
from dotenv import load_dotenv
from google.cloud.storage import Bucket

from ftb_s3_handler.gcs_client import GCSClient
from ftb_s3_handler.s3_client import S3Client

if __name__ == "__main__":
    load_dotenv()

    gcs_client = GCSClient().get_gcs_client()
    s3_client = S3Client().get_s3_client()
    bucket_id = os.environ["BUCKET_ID"]

    bucket: Bucket = gcs_client.bucket(bucket_id)
    blobs = bucket.list_blobs()

    for blob in blobs:

        year = blob.name.split("_")[1]
        content = blob.download_as_bytes()

        s3_client.put_object(
            Bucket="gcp-bigquery-stalse-landing",
            Key="api-project-1033684201634/analytics_153835980/events/teste/teste.csv",
            Body=content,
        )

        del content
        gc.collect()

        break