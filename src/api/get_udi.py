# src/api/get_udi.py

import requests
import os
import sys
import tempfile # Nécessaire pour le contournement GCSFS
from datetime import datetime
import pandas as pd
from typing import Dict, Any, List

# Imports Cloud essentiels
from google.cloud import storage 
from config import GCS_BUCKET_NAME 


# URL de base de l'API Hubeau
BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/"
ENDPOINT = "communes_udi"

# ----------------------------------------------------------------------
# Fonction de Pagination (Réinsérer cette fonction !)
# ----------------------------------------------------------------------

def get_data_from_endpoint_paginated(params: dict = {}) -> list:
    """
    Récupère toutes les données d'un point de terminaison de l'API en gérant la pagination.
    """
    url = f"{BASE_URL}{ENDPOINT}"
    print(f"-> Récupération des données depuis l'endpoint : {url}")
    
    all_data = []
    page = 1
    total_count = None
    
    params['size'] = 20000 

    while True:
        current_params = params.copy()
        current_params['page'] = page

        try:
            response = requests.get(url, params=current_params, timeout=60)
            response.raise_for_status() 
            
            data = response.json()
            results = data.get('data', [])
            total_count = data.get('count', 0)
            
            all_data.extend(results)
            print(f"   -> Page {page} récupérée. Total récupéré : {len(all_data)} sur {total_count}")
            
            if len(all_data) >= total_count:
                print("   -> Toutes les données ont été récupérées.")
                break
            
            page += 1

        except requests.exceptions.Timeout:
            print(f"Erreur de timeout après 60 secondes pour la page {page}.")
            break
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête vers {url}: {e}")
            break
            
    return all_data

# ----------------------------------------------------------------------
# Fonction d'Orchestration (Sauvegarde par Client Natif)
# ----------------------------------------------------------------------

def main_cloud_ready():
    """
    Orchestre l'extraction des UDI et les sauvegarde en Parquet sur GCS
    via le client natif Google Cloud Storage (Contournement de GCSFS).
    """
    if not GCS_BUCKET_NAME:
        print("❌ Échec de l'extraction : GCS_BUCKET_NAME est manquant.")
        sys.exit(1)

    print("Début du processus de récupération des UDI du département du Nord (59).")
    params = {"code_departement": "59"}
    # L'erreur se produit à la ligne suivante:
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
            
            # Sauvegarde locale du DataFrame en Parquet
            print(f"\n🔄 Sauvegarde temporaire locale de {len(df)} lignes vers {temp_local_path}")
            df.to_parquet(temp_local_path, index=False, engine='pyarrow', compression='snappy')
            
            # Utilisation du client GCS natif (plus stable)
            print(f"🔄 Début de l'envoi vers gs://{GCS_BUCKET_NAME}/{gcs_object_name}")
            storage_client = storage.Client()
            bucket = storage_client.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(gcs_object_name)
            
            # Uploader le fichier temporaire
            blob.upload_from_filename(temp_local_path)
            
            print(f"✅ Données UDI sauvegardées dans GCS : {gcs_object_name}")
            print(f"Total des enregistrements sauvegardés : {len(df)}\n")

    except Exception as e:
        print(f"❌ Erreur CRITIQUE lors de la sauvegarde GCS par client natif.")
        print(f"Détails de l'erreur : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main_cloud_ready()