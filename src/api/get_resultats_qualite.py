# src/api/get_resultats_qualite.py

import requests
import json
import os
import sys
from datetime import datetime
from google.cloud import storage 
import pandas as pd
from typing import Dict, Any, List
from config import GCS_BUCKET_NAME 
import time

# URL du point de terminaison pour les résultats d'analyse
BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/"
ENDPOINT = "resultats_dis"

# ----------------------------------------------------------------------
# Fonction d'Extraction (Pagination)
# ----------------------------------------------------------------------

def get_data_from_endpoint_paginated(params: Dict[str, Any] = {}) -> List[Dict[str, Any]]:
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
            response = requests.get(url, params=current_params)
            response.raise_for_status() # Lève une exception si le statut est une erreur (4xx ou 5xx)

            data = response.json()
            results = data.get('data', [])
            total_count = data.get('count', 0)

            all_data.extend(results)
            print(f"   -> Page {page} récupérée. Total: {len(all_data)} sur {total_count}")

            # Condition d'arrêt de la boucle
            if len(all_data) >= total_count:
                print("   -> Toutes les données ont été récupérées.")
                break

            page += 1

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête vers {url}: {e}")
            break

    return all_data

# ----------------------------------------------------------------------
# Fonction d'Orchestration (Sauvegarde sur GCS)
# ----------------------------------------------------------------------

def main():
    if GCS_BUCKET_NAME == "YOUR_DEFAULT_BUCKET_NAME_HERE":
        print("❌ Erreur: Veuillez configurer GCS_BUCKET_NAME dans config.py ou dans vos variables d'environnement.")
        sys.exit(1)

    # Paramètres spécifiques pour le département 59
    params = {"code_departement": "59"}

    print("Début du processus de récupération des résultats de qualité de l'eau pour le Nord (59).")
    data = get_data_from_endpoint_paginated(params)

    if data:
        df = pd.DataFrame(data)

        # Définition du chemin GCS pour le stockage du RAW Data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Chemin GCS : gs://VOTRE_BUCKET/raw/qualite_eau_YYYYMMDD_HHMMSS.parquet
        gcs_path = f"gs://{GCS_BUCKET_NAME}/raw/qualite_eau_{timestamp}.parquet"

        print(f"\n🔄 Sauvegarde du DataFrame ({len(df)} lignes) vers GCS : {gcs_path}")

        # Sauvegarde en Parquet sur GCS (Pandas utilise 'gcsfs' automatiquement pour gs://)
        try:
            df.to_parquet(gcs_path, index=False, engine='pyarrow', compression='snappy')
            print(f"✅ Données de qualité sauvegardées dans GCS : {gcs_path}")
            print(f"Total des enregistrements sauvegardés : {len(df)}\n")

        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde GCS. Vérifiez vos permissions et la configuration PyArrow/gcsfs.")
            print(f"Détails de l'erreur : {e}")
            sys.exit(1)
    else:
        print("❌ Aucune donnée n'a été récupérée. L'extraction s'arrête.")
        sys.exit(1)

if __name__ == "__main__":
    main_cloud_ready()