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
    s3_landing = os.environ["S3_LANDING"]

    bucket: Bucket = gcs_client.bucket(bucket_id)
    blobs = bucket.list_blobs()

    for blob in blobs:

        year = blob.name.split("_")[1]
        content = blob.download_as_bytes()
        key = s3_landing.format(year, blob.name)

        s3_client.put_object(
            Bucket="gcp-bigquery-stalse-landing",
            Key=key,
            Body=content,
        )

        del content
        gc.collect()