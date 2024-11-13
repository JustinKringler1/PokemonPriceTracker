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
def update_gcs_uri_in_bigquery(row_id, gcs_uri):
    update_query = f"""
    UPDATE `{TABLE_ID}`
    SET GCS_URI = @gcs_uri
    WHERE row_id = @row_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri),
            bigquery.ScalarQueryParameter("row_id", "STRING", row_id),
        ]
    )
    bigquery_client.query(update_query, job_config=job_config).result()
    print(f"Updated GCS URI for row ID {row_id}")

# Main function to process images
def process_images():
    # Query BigQuery for rows where GCS_URI is NULL
    query = f"""
    SELECT row_id, Product_Name, Image
    FROM `{TABLE_ID}`
    WHERE GCS_URI IS NULL
    """
    rows = bigquery_client.query(query).result()

    for row in rows:
        image_url = row["Image"]
        row_id = row["row_id"]
        filename = f"{row['Product_Name'].replace(' ', '_')}_{row_id}.jpg"

        # Download and upload the image
        gcs_uri = download_and_upload_image(image_url, filename)

        # Update BigQuery with the GCS URI if successful
        if gcs_uri:
            update_gcs_uri_in_bigquery(row_id, gcs_uri)

# Run the script
if __name__ == "__main__":
    process_images()
