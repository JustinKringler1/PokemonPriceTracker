import os
import requests
from io import BytesIO
from google.cloud import storage, bigquery
from datetime import datetime

# Constants
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")  # Get bucket name from environment
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = "pokemon_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.pokemon_images"

# Initialize Google Cloud clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Function to download an image and upload it to GCS
def download_and_upload_image(image_url, filename):
    try:
        # Download image into memory
        response = requests.get(image_url)
        response.raise_for_status()
        image_bytes = BytesIO(response.content)

        # Upload the image to GCS directly from memory
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"images/{filename}")
        blob.upload_from_file(image_bytes, content_type="image/jpeg")

        # Return the GCS URI of the uploaded image
        return f"gs://{BUCKET_NAME}/images/{filename}"
    
    except Exception as e:
        print(f"Failed to download or upload {image_url}: {e}")
        return None

# Function to update the GCS URI in BigQuery for each row
def update_GCS_URI_in_bigquery(id, GCS_URI):
    update_query = f"""
    UPDATE `{TABLE_ID}`
    SET gcs_uri = @GCS_URI
    WHERE id = @id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("GCS_URI", "STRING", GCS_URI),
            bigquery.ScalarQueryParameter("id", "STRING", id),
        ]
    )
    bigquery_client.query(update_query, job_config=job_config).result()
    print(f"Updated GCS URI for row ID {id}")

# Main function to process images
def process_images():
    # Query BigQuery for rows where gcs_uri is NULL
    query = f"""
    SELECT id, `Product Name`, Image
    FROM `{TABLE_ID}`
    WHERE gcs_uri IS NULL
    """
    rows = bigquery_client.query(query).result()

    for row in rows:
        image_url = row["Image"]
        id = row["id"]
        filename = f"{row['Product Name'].replace(' ', '_')}_{id}.jpg"

        # Download and upload the image
        GCS_URI = download_and_upload_image(image_url, filename)

        # Update BigQuery with the GCS URI if successful
        if GCS_URI:
            update_GCS_URI_in_bigquery(id, GCS_URI)

# Run the script
if __name__ == "__main__":
    process_images()
