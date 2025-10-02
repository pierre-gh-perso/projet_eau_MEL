# src/load/load_to_bq.py

import pandas as pd
import sys
import os
from google.cloud import bigquery
from google.api_core import exceptions
import gcsfs # Assurez-vous d'avoir 'pip install gcsfs'

# Importation des variables d'environnement de la configuration
try:
    from config import GCS_BUCKET_NAME, BQ_DATASET_ID, GCP_PROJECT_ID
except ImportError:
    # Si config.py n'est pas trouvé (peu probable dans le workflow, mais bonne pratique)
    print("Erreur critique: Impossible d'importer les variables de configuration.")
    sys.exit(1)


def load_processed_data_to_bigquery(project_id: str, dataset_id: str, gcs_bucket: str):
    """
    Lit les 4 tables Parquet depuis GCS/processed et les charge dans BigQuery.
    """
    if not all([project_id, gcs_bucket, dataset_id]):
        print("Erreur: Les variables Project ID, Bucket Name ou Dataset ID sont manquantes.")
        sys.exit(1)

    # 1. Connexion et Vérification du Dataset
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    print(f"🔄 Connexion à BigQuery réussie. Projet : {project_id}")

    try:
        # Créer le dataset s'il n'existe pas
        client.get_dataset(dataset_ref)
        print(f"   Dataset '{dataset_id}' existe déjà.")
    except exceptions.NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "europe-west1"  # Utilisez la même localisation que votre bucket
        client.create_dataset(dataset)
        print(f"   ✅ Dataset '{dataset_id}' créé.")

    # Les 4 noms de tables à charger
    table_names = ['prelevements', 'parametres', 'resultats_mesures', 'communes_udi']
    
    # 2. Chargement des tables
    for table_name in table_names:
        
        # Le chemin exact est connu et unique car l'étape de transformation écrit
        # un seul fichier Parquet par nom de table dans le dossier 'processed'
        gcs_file_path = f"gs://{gcs_bucket}/processed/{table_name}.parquet"
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        try:
            print(f"\n🔄 Tentative de lecture de la table '{table_name}' depuis {gcs_file_path}")
            
            # --- Utilisation de BigQuery pour charger directement depuis GCS (Méthode 1: Rapide et Évolutive) ---
            # NOTE: C'est l'approche la plus performante pour charger de gros volumes.
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE", # Remplace la table à chaque exécution
                # Schéma: BigQuery déduit le schéma depuis le fichier Parquet.
            )
            
            # Démarrage du job de chargement
            load_job = client.load_table_from_uri(
                gcs_file_path, 
                table_id, 
                job_config=job_config
            )
            
            print(f"   -> Chargement BQ démarré. Job ID: {load_job.job_id}")
            load_job.result()  # Attend la fin du job
            
            # Affichage du résultat
            print(f"   ✅ Table {table_name} chargée. {load_job.output_rows} lignes écrites.")

        except exceptions.NotFound as e:
            # Lève une exception si le fichier Parquet est introuvable sur GCS
            print(f"   ❌ Échec: Fichier Parquet non trouvé sur GCS : {e}")
            raise  # Arrête le pipeline si une table essentielle manque
        except Exception as e:
            print(f"   ❌ Échec critique du chargement BQ pour {table_name}: {e}")
            raise # Arrête le pipeline


def main():
    """
    Point d'entrée principal du module de chargement.
    """
    # L'authentification est gérée par GitHub Actions, le client BQ utilise le contexte
    load_processed_data_to_bigquery(GCP_PROJECT_ID, BQ_DATASET_ID, GCS_BUCKET_NAME)

if __name__ == "__main__":
    main()