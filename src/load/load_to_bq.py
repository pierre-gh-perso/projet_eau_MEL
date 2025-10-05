# src/load/load_to_bq.py

import pandas as pd
import sys
import os
import io
from google.cloud import bigquery
from google.api_core import exceptions
import gcsfs 
from typing import List, Dict


# Importation des variables d'environnement de la configuration
try:
    from config import GCS_BUCKET_NAME, BQ_DATASET_ID, GCP_PROJECT_ID
except ImportError:
    print("Erreur critique: Impossible d'importer les variables de configuration.")
    sys.exit(1)

# Initialisation du client BQ
client = bigquery.Client(project=GCP_PROJECT_ID)

# Clés primaires des tables pour la déduplication (uniquement pour les tables APPEND)
# Pour une déduplication parfaite sur toutes les tables, la clé doit être définie ici.
# Pour l'étape APPEND, on se concentre sur la table de Faits.
TABLE_PRIMARY_KEYS: Dict[str, List[str]] = {
    'resultats_mesures': ['code_prelevement', 'code_parametre'] # Clé composite pour les mesures
    # Les dimensions (prelevements, parametres, communes_reseau) seront TRUNCATE (WRITE_TRUNCATE)
}

# --- NOUVELLE FONCTION : LECTURE GCS & DÉDUPLICATION ---

def load_parquet_and_deduplicate(gcs_file_path: str, table_name: str, primary_keys: List[str], bq_table_id: str) -> pd.DataFrame:
    """
    Lit un fichier Parquet depuis GCS en mémoire et le déduplique
    en comparant les clés primaires avec celles déjà présentes dans BigQuery.
    """
    # 1. Lecture du Parquet dans Pandas (Nécessaire pour la déduplication)
    try:
        fs = gcsfs.GCSFileSystem()
        with fs.open(gcs_file_path, 'rb') as f:
            df = pd.read_parquet(io.BytesIO(f.read()))
    except FileNotFoundError as e:
        print(f"   ❌ Fichier GCS non trouvé à l'emplacement : {gcs_file_path}")
        raise e
    
    initial_count = len(df)
    print(f"   -> {initial_count} lignes lues depuis GCS. Début de la vérification des doublons...")

    # 2. Déduplication pour les tables en mode APPEND (Faits)
    if table_name in TABLE_PRIMARY_KEYS:
        # Créer une colonne clé temporaire pour le merge/filtrage
        df['temp_key'] = df[primary_keys].astype(str).agg('_'.join, axis=1)

        # Lire les clés existantes depuis BigQuery
        query = f"SELECT DISTINCT {', '.join(primary_keys)} FROM `{bq_table_id}`"
        try:
            existing_keys_df = client.query(query).to_dataframe()
            existing_keys_df['temp_key'] = existing_keys_df[primary_keys].astype(str).agg('_'.join, axis=1)
            
            # Filtrer : Garder les lignes dont la clé n'existe PAS dans BQ
            df_dedup = df[~df['temp_key'].isin(existing_keys_df['temp_key'])].drop(columns=['temp_key']).copy()
            
            duplicates_count = initial_count - len(df_dedup)
            
            if duplicates_count > 0:
                print(f"   🗑️ {duplicates_count} lignes en doublon identifiées (clés : {primary_keys}) et supprimées.")
            
            return df_dedup
            
        except exceptions.NotFound:
            print(f"   ⚠️ Table BQ '{bq_table_id}' non trouvée (première exécution ?). Toutes les lignes seront chargées.")
            return df.drop(columns=['temp_key']).copy()

    # 3. Pour les tables de Dimensions (mode TRUNCATE), la déduplication est implicite.
    return df


# --- FONCTION PRINCIPALE DE CHARGEMENT BIGQUERY (MODIFIÉE) ---

def load_processed_data_to_bigquery(project_id: str, dataset_id: str, gcs_bucket: str):
    """
    Lit les 4 tables Parquet depuis GCS/processed, les déduplique et les charge dans BigQuery.
    """
    if not all([project_id, gcs_bucket, dataset_id]):
        print("Erreur: Les variables Project ID, Bucket Name ou Dataset ID sont manquantes.")
        sys.exit(1)

    # 1. Connexion et Vérification du Dataset (Inchangé)
    dataset_ref = client.dataset(dataset_id)
    print(f"🔄 Connexion à BigQuery réussie. Projet : {project_id}")

    try:
        client.get_dataset(dataset_ref)
        print(f"   Dataset '{dataset_id}' existe déjà.")
    except exceptions.NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "europe-west1"
        client.create_dataset(dataset)
        print(f"   ✅ Dataset '{dataset_id}' créé.")

    # Les 4 noms de tables à charger
    table_names = ['prelevements', 'parametres', 'communes_reseau', 'resultats_mesures']
    
    # 2. Chargement des tables
    for table_name in table_names:
        
        gcs_file_path = f"gs://{gcs_bucket}/processed/{table_name}.parquet"
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        # Déterminer la disposition d'écriture
        write_mode_object = bigquery.WriteDisposition.WRITE_APPEND if table_name in TABLE_PRIMARY_KEYS else bigquery.WriteDisposition.WRITE_TRUNCATE
        
        write_mode_str = str(write_mode_object) 
        
        print(f"\n🔄 Traitement de la table '{table_name}' (Mode: {write_mode_str.split('_')[1]})")
        
        try:
            # A. LECTURE & DÉDUPLICATION
            df_to_load = load_parquet_and_deduplicate(
                gcs_file_path, 
                table_name, 
                TABLE_PRIMARY_KEYS.get(table_name, []),
                table_id
            )

            if df_to_load.empty:
                print(f"   ℹ️ Aucune nouvelle ligne à charger pour la table {table_name}. Skip.")
                continue

            # B. CHARGEMENT DANS BIGQUERY
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_mode_object, 
            )
            
            # load_table_from_dataframe
            load_job = client.load_table_from_dataframe(
                df_to_load, 
                table_id, 
                job_config=job_config
            )
            
            print(f"   -> Chargement BQ démarré. Job ID: {load_job.job_id}")
            load_job.result()
            
            print(f"   ✅ Table {table_name} chargée. {load_job.output_rows} lignes écrites.")

        except exceptions.NotFound:
            print(f"   ❌ Erreur: Le fichier {gcs_file_path} est introuvable. Vérifiez l'étape de transformation.")
            raise
        except Exception as e:
            print(f"   ❌ Échec critique du chargement BQ pour {table_name}: {e}")
            raise


def main():
    """
    Point d'entrée principal du module de chargement.
    """
    print("--- Démarrage de l'Étape 3: Chargement BigQuery ---")
    load_processed_data_to_bigquery(GCP_PROJECT_ID, BQ_DATASET_ID, GCS_BUCKET_NAME)
    print("--- Fin de l'Étape 3: Chargement BigQuery terminé. ---")

if __name__ == "__main__":
    main()