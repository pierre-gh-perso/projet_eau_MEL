# src/load/load_to_bq.py

import pandas as pd
from google.cloud import bigquery
from config import GCS_BUCKET_NAME, BQ_DATASET_ID, GCP_PROJECT_ID
import os
import glob

def load_processed_data_to_bigquery(project_id: str, dataset_id: str, gcs_bucket: str):
    """
    Lit les 4 tables Parquet depuis GCS et les charge dans BigQuery.
    """
    client = bigquery.Client(project=project_id)
    print(f"🔄 Connexion à BigQuery réussie. Projet : {project_id}")

    # Les 4 noms de tables que nous avons définis dans le MLD
    table_names = ['prelevements', 'parametres_mesures', 'parametres', 'communes_udi']
    
    # Trouver le dernier dossier processed sur GCS (nécessite une autre logique que glob.glob)
    # Pour simplifier dans l'action, nous allons simplement lister les fichiers les plus récents.
    
    # ----------------------------------------------------
    # NOTE: Pour la production, vous devez lister les fichiers
    # sur GCS et identifier l'ensemble de fichiers le plus récent.
    # Ici, nous supposons qu'un script d'orchestration plus haut
    # vous a donné le chemin complet des fichiers Parquet.
    # ----------------------------------------------------

    for table_name in table_names:
        # On lit le dernier fichier Parquet de ce type dans le dossier processed/
        # (Cette ligne suppose que vous avez un moyen de connaître le chemin exact,
        # ou qu'il n'y a qu'un seul jeu de données 'processed' actif).
        
        # Pour le test, on va simuler la lecture du fichier
        # Chemin complet vers le fichier Parquet dans GCS
        gcs_file_pattern = f"gs://{gcs_bucket}/processed/{table_name}*.parquet"
        
        # NOTE: La lecture via pandas.read_parquet directement depuis un pattern est simple.
        try:
            df = pd.read_parquet(gcs_file_pattern)
            
            # Définition de la table BigQuery (ex: eau_potable_mel.prelevements)
            table_id = f"{project_id}.{dataset_id}.{table_name}"
            
            print(f"   -> Chargement de {len(df)} lignes dans la table {table_id}...")

            # Chargement du DataFrame dans BigQuery
            job = client.load_table_from_dataframe(
                df, 
                table_id, 
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") # Remplacer la table
            )
            job.result() # Attend que le job se termine
            
            print(f"      ✅ Table {table_name} chargée. Temps écoulé: {job.ended - job.started}")

        except Exception as e:
            print(f"      ❌ Échec du chargement de la table {table_name}: {e}")
            raise # Lève l'exception pour que GitHub Actions échoue

def main():
    if not all([GCP_PROJECT_ID, GCS_BUCKET_NAME, BQ_DATASET_ID]):
        print("Erreur: Variables d'environnement GCP manquantes dans config.py.")
        sys.exit(1)
        
    load_processed_data_to_bigquery(GCP_PROJECT_ID, BQ_DATASET_ID, GCS_BUCKET_NAME)

if __name__ == "__main__":
    main()