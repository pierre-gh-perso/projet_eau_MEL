# src/api/get_udi.py

import requests
import os
import sys
from datetime import datetime
import pandas as pd
from typing import Dict, Any, List

# Imports Cloud essentiels
import gcsfs
# Importation de la librairie google.cloud.storage pour assurer l'initialisation du contexte
from google.cloud import storage 
from config import GCS_BUCKET_NAME, GCP_PROJECT_ID 


# URL de base de l'API Hubeau
BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/"
ENDPOINT = "communes_udi"

# ----------------------------------------------------------------------
# Fonction de Pagination 
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
    
    params['size'] = 20000 # Taille maximale des pages

    while True:
        current_params = params.copy()
        current_params['page'] = page

        try:
            # Ajout d'un timeout
            response = requests.get(url, params=current_params, timeout=60)
            response.raise_for_status() 
            
            data = response.json()
            results = data.get('data', [])
            total_count = data.get('count', 0)
            
            all_data.extend(results)
            print(f"   -> Page {page} récupérée. Total récupéré : {len(all_data)} sur {total_count}")
            
            # Condition d'arrêt
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

def main_cloud_ready():
    """
    Orchestre l'extraction des UDI et les sauvegarde en Parquet sur GCS.
    """
    if not GCS_BUCKET_NAME or not GCP_PROJECT_ID:
        print("❌ Échec de l'extraction : GCS_BUCKET_NAME ou GCP_PROJECT_ID sont manquants.")
        sys.exit(1)

    # NOUVEAU: Initialisation explicite de la session GCSFS (Correction de l'erreur 'b***/o/raw')
    try:
        # Ceci force gcsfs à utiliser le contexte d'authentification fourni par GitHub Actions
        fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
        print("DEBUG: Contexte GCSFS initialisé avec succès pour la lecture/écriture Cloud.")
    except Exception as e:
        print(f"FATAL: Échec de l'initialisation GCSFS : {e}")
        sys.exit(1)


    # Paramètres spécifiques pour le département 59
    params = {"code_departement": "59"}
    
    print("Début du processus de récupération des UDI du département du Nord (59).")
    data = get_data_from_endpoint_paginated(params)
    
    if data:
        df = pd.DataFrame(data)

        # Définition du chemin GCS
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        gcs_path = f"gs://{GCS_BUCKET_NAME}/raw/udi_mel_{timestamp}.parquet"

        print(f"\n🔄 Sauvegarde du DataFrame ({len(df)} lignes) vers GCS : {gcs_path}")

        # Sauvegarde en Parquet sur GCS
        try:
            # Pandas utilise l'instance gcsfs configurée en arrière-plan
            df.to_parquet(gcs_path, index=False, engine='pyarrow', compression='snappy')
            print(f"✅ Données UDI sauvegardées dans GCS : {gcs_path}")
            print(f"Total des enregistrements sauvegardés : {len(df)}\n")

        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde GCS. Vérifiez les permissions du Compte de Service.")
            print(f"Détails de l'erreur : {e}")
            sys.exit(1)
    else:
        print("❌ Aucune donnée n'a été récupérée. L'extraction s'arrête.")
        sys.exit(1)

if __name__ == "__main__":
    main_cloud_ready()