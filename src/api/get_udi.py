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
# Assurez-vous que config.py est à jour avec les variables d'environnement
from config import GCS_BUCKET_NAME, GCP_PROJECT_ID, GCP_CREDENTIALS_PATH


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

def main_cloud_ready():
    """
    Orchestre l'extraction des UDI et les sauvegarde en Parquet sur GCS.
    """
    if not GCS_BUCKET_NAME or not GCP_PROJECT_ID:
        print("❌ Échec de l'extraction : GCS_BUCKET_NAME ou GCP_PROJECT_ID sont manquants.")
        sys.exit(1)

    # BLOC DE DÉBOGAGE CRITIQUE GCSFS (Pour capturer l'erreur cryptique)
    try:
        print(f"DEBUG GCS: Initialisation avec identifiants explicites...")
        
        # 1. Utiliser le chemin du fichier JSON pour l'authentification
        if not GCP_CREDENTIALS_PATH:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS n'est pas défini.")
            
        # 2. Forcer gcsfs à utiliser le fichier de clés JSON (authentification la plus fiable)
        fs = gcsfs.GCSFileSystem(
            project=GCP_PROJECT_ID,
            token=GCP_CREDENTIALS_PATH # <- LA CLÉ : Passe directement le chemin JSON
        )
        
        # 3. Test de connexion (maintenant qu'il est correctement authentifié)
        # On peut laisser le test puisqu'on sait que le dossier raw existe.
        fs.ls(f"{GCS_BUCKET_NAME}/raw/")
        
        print("DEBUG GCS: Connexion GCS réussie via fichier de clés JSON. Extraction autorisée.")
    except Exception as e:
        # Ceci devrait arrêter de se produire !
        print(f"FATAL: Échec CRITIQUE de la connexion GCSFS. Détails: {e}")
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