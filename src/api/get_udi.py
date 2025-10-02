# src/api/get_udi.py

import requests
import os
import sys
import tempfile # Pour créer un fichier temporaire
from datetime import datetime
import pandas as pd

# Imports Cloud essentiels
# Nous n'utiliserons plus gcsfs, mais le client natif
from google.cloud import storage 
from config import GCS_BUCKET_NAME 
# Nous n'avons plus besoin de GCP_PROJECT_ID et GCP_CREDENTIALS_PATH ici, 
# car le client 'storage' se charge seul de l'authentification dans l'environnement GHA.

# URL de base de l'API Hubeau
BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/"
ENDPOINT = "communes_udi"

# ... (Fonction get_data_from_endpoint_paginated inchangée) ...

# ----------------------------------------------------------------------
# Fonction d'Orchestration (Sauvegarde par Client Natif)
# ----------------------------------------------------------------------

def main_cloud_ready():
    """
    Orchestre l'extraction des UDI et les sauvegarde en Parquet sur GCS
    via le client natif Google Cloud Storage.
    """
    if not GCS_BUCKET_NAME:
        print("❌ Échec de l'extraction : GCS_BUCKET_NAME est manquant.")
        sys.exit(1)

    print("Début du processus de récupération des UDI du département du Nord (59).")
    params = {"code_departement": "59"}
    data = get_data_from_endpoint_paginated(params)
    
    if not data:
        print("❌ Aucune donnée n'a été récupérée. L'extraction s'arrête.")
        sys.exit(1)

    df = pd.DataFrame(data)
    
    # 1. Préparation des chemins
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    gcs_object_name = f"raw/udi_mel_{timestamp}.parquet"
    
    # --- CONTOURNEMENT DE GCSFS ---
    try:
        # Créer un fichier temporaire sur le runner GHA
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_local_path = os.path.join(tmpdir, "udi_temp.parquet")
            
            # Sauvegarde locale du DataFrame en Parquet (utilise PyArrow)
            print(f"\n🔄 Sauvegarde temporaire locale de {len(df)} lignes vers {temp_local_path}")
            df.to_parquet(temp_local_path, index=False, engine='pyarrow', compression='snappy')
            
            # Utilisation du client GCS natif (plus stable pour l'authentification)
            print(f"🔄 Début de l'envoi vers gs://{GCS_BUCKET_NAME}/{gcs_object_name}")
            storage_client = storage.Client()
            bucket = storage_client.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(gcs_object_name)
            
            # Uploader le fichier temporaire
            blob.upload_from_filename(temp_local_path)
            
            print(f"✅ Données UDI sauvegardées dans GCS : {gcs_object_name}")
            print(f"Total des enregistrements sauvegardés : {len(df)}\n")

    except Exception as e:
        # Si cette étape échoue, c'est VRAIMENT un problème de permission
        print(f"❌ Erreur CRITIQUE lors de la sauvegarde GCS par client natif.")
        print(f"Détails de l'erreur : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main_cloud_ready()