# src/api/get_udi.py

import requests
import os
import sys
from datetime import datetime
import pandas as pd
from typing import Dict, Any, List

# Imports Cloud essentiels
import gcsfs
from google.cloud import storage 
from config import GCS_BUCKET_NAME, GCP_PROJECT_ID 


# URL de base de l'API Hubeau
BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/"
ENDPOINT = "communes_udi"

# ----------------------------------------------------------------------
# Fonction de Pagination (Inchangée)
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
# Fonction d'Orchestration (Sauvegarde sur GCS)
# ----------------------------------------------------------------------

def main():
    """
    Orchestre l'extraction des UDI et les sauvegarde en Parquet sur GCS.
    """
    if not GCS_BUCKET_NAME or not GCP_PROJECT_ID:
        print("❌ Échec de l'extraction : GCS_BUCKET_NAME ou GCP_PROJECT_ID sont manquants.")
        sys.exit(1)

    # NOUVEAU: BLOC DE DÉBOGAGE CRITIQUE GCSFS (Pour capturer l'erreur cryptique)
    try:
        print(f"DEBUG GCS: Tentative d'initialisation du système de fichiers pour le projet : {GCP_PROJECT_ID}")
        
        # 1. Initialisation GCSFS
        fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
        
        # 2. Test de connexion simple (listage du dossier 'raw/')
        # Si le listage échoue, c'est que l'authentification ou le chemin est incorrect
        fs.ls(f"{GCS_BUCKET_NAME}/raw/")
        
        print("DEBUG GCS: Connexion et listage du dossier 'raw/' réussis. Poursuite de l'extraction.")
    except Exception as e:
        # Ceci devrait afficher la VRAIE cause de l'erreur 'b***/o/raw'
        print(f"FATAL: Échec critique de la connexion GCSFS. Ceci est la cause de l'erreur 'b***/o/raw'.")
        print(f"Détails de l'erreur GCSFS : {e}")
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
            # Cette ligne est le point de défaillance précédent
            df.to_parquet(gcs_path, index=False, engine='pyarrow', compression='snappy')
            print(f"✅ Données UDI sauvegardées dans GCS : {gcs_path}")
            print(f"Total des enregistrements sauvegardés : {len(df)}\n")

        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde GCS. La connexion a réussi, mais l'écriture a échoué. Détails : {e}")
            sys.exit(1)
    else:
        print("❌ Aucune donnée n'a été récupérée. L'extraction s'arrête.")
        sys.exit(1)

if __name__ == "__main__":
    main_cloud_ready()