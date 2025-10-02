# src/api/get_udi.py

import requests
import json
import os
import sys
from datetime import datetime
from google.cloud import storage 
import pandas as pd
from typing import Dict, Any, List

# Assurez-vous d'avoir installé 'gcsfs' (pip install gcsfs)
# car pandas utilise cette librairie pour interagir avec gs://

# URL de base de l'API Hubeau
BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/"

# Importation du nom du bucket depuis la configuration
try:
    from config import GCS_BUCKET_NAME 
except ImportError:
    # Fallback pour le développement local si config.py est incomplet
    GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "YOUR_DEFAULT_BUCKET_NAME")
    if GCS_BUCKET_NAME == "YOUR_DEFAULT_BUCKET_NAME":
        print("Avertissement: GCS_BUCKET_NAME non configuré dans config.py ni dans les variables d'environnement.")

# ----------------------------------------------------------------------
# Fonction de Pagination (Réutilisée)
# ----------------------------------------------------------------------

def get_data_from_endpoint_paginated(endpoint: str, params: dict = {}) -> list:
    """
    Récupère toutes les données d'un point de terminaison de l'API en gérant la pagination.
    """
    url = f"{BASE_URL}{endpoint}"
    print(f"-> Récupération des données depuis l'endpoint : {url}")
    
    all_data = []
    page = 1
    total_count = None
    
    # On définit la taille maximale des pages pour réduire le nombre de requêtes
    params['size'] = 20000

    while True:
        current_params = params.copy()
        current_params['page'] = page

        try:
            # Ajout d'un timeout de 60 secondes pour éviter le blocage du runner
            response = requests.get(url, params=current_params, timeout=60)
            response.raise_for_status() 
            
            data = response.json()
            results = data.get('data', [])
            total_count = data.get('count', 0)
            
            all_data.extend(results)
            print(f"   -> Page {page} récupérée. Enregistrements : {len(results)}. Total récupéré : {len(all_data)} sur {total_count}")
            
            # Condition d'arrêt de la boucle
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
# Fonction d'Orchestration (Sauvegarde sur GCS)
# ----------------------------------------------------------------------

def main():
    """
    Orchestre l'extraction des UDI et les sauvegarde en Parquet sur GCS.
    """
    if not GCS_BUCKET_NAME or "YOUR_DEFAULT_BUCKET_NAME" in GCS_BUCKET_NAME:
        print("❌ Échec de l l'extraction : GCS_BUCKET_NAME est mal configuré.")
        sys.exit(1)

    # Paramètres spécifiques pour l'endpoint communes_udi et le département 59
    endpoint = "communes_udi"
    params = {"code_departement": "59"}
    
    print("Début du processus de récupération des UDI du département du Nord (59).")
    data = get_data_from_endpoint_paginated(endpoint, params)
    
    if data:
        df = pd.DataFrame(data)

        # Définition du chemin GCS pour le stockage du RAW Data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Chemin GCS : gs://VOTRE_BUCKET/raw/udi_mel_YYYYMMDD_HHMMSS.parquet
        gcs_path = f"gs://{GCS_BUCKET_NAME}/raw/udi_mel_{timestamp}.parquet"

        print(f"\n🔄 Sauvegarde du DataFrame ({len(df)} lignes) vers GCS : {gcs_path}")

        # Sauvegarde en Parquet sur GCS
        try:
            df.to_parquet(gcs_path, index=False, engine='pyarrow', compression='snappy')
            print(f"✅ Données UDI sauvegardées dans GCS : {gcs_path}")
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