# tendo que estragar meu repo pq sim
import logging
import os
import gc
from dotenv import load_dotenv
from google.cloud.storage import Bucket

from ftb_s3_handler.gcs_client import GCSClient
from ftb_s3_handler.s3_client import S3Client

logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    load_dotenv()

    gcs_client = GCSClient().get_gcs_client()
    s3_client = S3Client().get_s3_client()
    bucket_id = os.environ["BUCKET_ID"]
    s3_landing = os.environ["S3_LANDING"]
    s3_bucket = os.environ["S3_BUCKET"]

    bucket: Bucket = gcs_client.bucket(bucket_id)
    blobs = bucket.list_blobs()

    for blob in blobs:

        logger.info("Downloading {0}/{1}".format(bucket_id, blob.name))

        content = blob.download_as_bytes()
        key = s3_landing.format(blob.name)

        logger.info("Sending {0}/{1} to {2}/{3}".format(bucket_id, blob.name, s3_bucket, key))

        s3_client.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=content,
        )

        logger.info("Cleaning {0}/{1}".format(bucket_id, blob.name))

        del content
        gc.collect()