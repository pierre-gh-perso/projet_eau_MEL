# config.py

import os

GCP_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME") 

BQ_DATASET_ID = "eau_potable_mel" 

if not GCP_PROJECT_ID:
    print("FATAL: La variable d'environnement GCP_PROJECT_ID n'est pas définie.")