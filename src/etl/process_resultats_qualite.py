# src/etl/process_resultats_qualite.py

import pandas as pd
import sys
import gcsfs 
import os
import io
from google.cloud import storage 
from config import GCS_BUCKET_NAME, GCP_PROJECT_ID
from typing import Dict, Any, List
from datetime import datetime

# ----------------------------------------------------------------------
# Liste statique des codes INSEE de la MEL (Inchangée)
# ----------------------------------------------------------------------
MEL_COMMUNES_INSEE = [
    '59001', '59004', '59008', '59011', '59017', '59018', '59020', '59021', '59032', '59045', 
    '59046', '59048', '59049', '59051', '59056', '59057', '59063', '59065', '59067', '59074', 
    '59079', '59082', '59087', '59092', '59098', '59101', '59103', '59104', '59107', '59112', 
    '59124', '59129', '59130', '59132', '59133', '59134', '59152', '59154', '59163', '59166', 
    '59178', '59183', '59189', '59190', '59207', '59208', '59214', '59223', '59224', '59226', 
    '59230', '59235', '59236', '59247', '59248', '59260', '59265', '59267', '59274', '59276', 
    '59281', '59286', '59294', '59296', '59300', '59306', '59312', '59316', '59330', '59341', 
    '59350', '59353', '59364', '59368', '59378', '59388', '59389', '59390', '59400', '59405', 
    '59408', '59410', '59416', '59424', '59429', '59441', '59443', '59451', '59452', '59459', 
    '59470', '59473', '59482', '59487', '59495', '59496', '59507', '59508', '59509', '59512', 
    '59518', '59520', '59526', '59534', '59549', '59550', '59552', '59560', '59564', '59579', 
    '59582', '59583', '59584', '59599', '59600', '59606', '59620', '59627', '59632', '59637', 
    '59646', '59652', '59653', '59660', '59667', '59669', '59670', '59671', '59675', '59676', 
    '59681', '59683', '59684', '59686', '59690', '59701', '59714', '59715' 
]

# Initialisation du client GCS (utilise ADC pour GitHub Actions)
storage_client = storage.Client()

# ----------------------------------------------------------------------
# Fonctions utilitaires GCS (AJOUT DE LA FONCTION DE NETTOYAGE)
# ----------------------------------------------------------------------

def get_latest_gcs_path(bucket_name: str, prefix: str, folder: str = "raw") -> str:
    """
    Trouve le nom d'objet GCS du fichier Parquet le plus récent.
    """
    bucket = storage_client.bucket(bucket_name)
    prefix_path = f"{folder}/{prefix}"
    
    blobs = list(bucket.list_blobs(prefix=prefix_path))
    target_blobs = [blob for blob in blobs if blob.name.endswith('.parquet')]
    
    if not target_blobs:
        raise FileNotFoundError(f"Aucun fichier avec le préfixe '{prefix_path}' n'a été trouvé dans le bucket.")
        
    target_blobs.sort(key=lambda blob: blob.name, reverse=True)
    return target_blobs[0].name

def read_parquet_from_gcs(bucket_name: str, object_name: str) -> pd.DataFrame:
    """
    Lit un fichier Parquet depuis GCS en mémoire.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    
    print(f"   -> Téléchargement de gs://{bucket_name}/{object_name}")
    blob_bytes = blob.download_as_bytes()
    
    return pd.read_parquet(io.BytesIO(blob_bytes))

def save_df_to_gcs(df: pd.DataFrame, bucket_name: str, table_name: str):
    """
    Sauvegarde un DataFrame en Parquet dans le dossier GCS/processed.
    """
    gcs_object_name = f"processed/{table_name}.parquet" 
    print(f"   -> Sauvegarde de {len(df)} lignes dans gs://{bucket_name}/{gcs_object_name}")
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', compression='snappy')
    buffer.seek(0)
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_object_name)
    blob.upload_from_file(buffer, content_type='application/octet-stream')
    
    print(f"   ✅ Table {table_name} sauvegardée.")

def cleanup_old_gcs_files(bucket_name: str, latest_object_names: List[str]):
    """
    Supprime toutes les versions antérieures des fichiers bruts dans GCS/raw.
    Garde uniquement les fichiers dont les noms sont dans latest_object_names.
    """
    bucket = storage_client.bucket(bucket_name)
    print("\n🧹 Début du nettoyage des anciennes données brutes (GCS/raw)...")
    
    # 1. Lister tous les blobs bruts
    all_raw_blobs = list(bucket.list_blobs(prefix="raw/"))
    
    # Extraire uniquement les noms d'objets (ex: 'raw/qualite_eau_20251004_070622.parquet')
    latest_blobs_to_keep = set(latest_object_names)
    
    # 2. Identifier les fichiers à supprimer
    blobs_to_delete = [
        blob for blob in all_raw_blobs 
        if blob.name.endswith('.parquet') and blob.name not in latest_blobs_to_keep
    ]
    
    if not blobs_to_delete:
        print("   -> Aucun ancien fichier brut trouvé à supprimer.")
        return

    # 3. Suppression
    for blob in blobs_to_delete:
        print(f"   -> Suppression de : gs://{bucket_name}/{blob.name}")
        blob.delete()
        
    print(f"   ✅ Nettoyage GCS terminé. {len(blobs_to_delete)} anciens fichiers supprimés.")


# ----------------------------------------------------------------------
# Logique de Transformation et Normalisation (Inchangée)
# ----------------------------------------------------------------------

def transform_and_normalize_data(df_qualite: pd.DataFrame, df_udi: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Filtre, nettoie et normalise les données brutes en tables finales.
    """
    print("   -> Début du nettoyage et de la normalisation...")
    # 1. Préparation et FILTRAGE des Résultats de Qualité (Table de faits principale)
    df_qualite['code_commune'] = df_qualite['code_commune'].astype(str).str.zfill(5)
    mel_qualite_df = df_qualite[df_qualite['code_commune'].isin(MEL_COMMUNES_INSEE)].copy()
    print(f"   -> Enregistrements filtrés pour la MEL : {len(mel_qualite_df)}")
    if mel_qualite_df.empty:
        raise ValueError("Aucun résultat de qualité trouvé pour les communes de la MEL après filtrage.")
    mel_qualite_df['code_prelevement'] = mel_qualite_df['code_prelevement'].astype(str)
    mel_qualite_df['code_parametre'] = mel_qualite_df['code_parametre'].astype(str)

    # 2. FILTRAGE du DF UDI (pour les infos de réseau/commune)
    df_udi['code_commune'] = df_udi['code_commune'].astype(str).str.zfill(5)
    mel_udi_df = df_udi[df_udi['code_commune'].isin(MEL_COMMUNES_INSEE)].copy()

    # --- Construction des 4 tables (Filtrées et Dénormalisées) ---
    params_cols = ['code_parametre', 'libelle_parametre', 'code_type_parametre', 'code_parametre_se', 'libelle_parametre_maj'] 
    df_parametres = mel_qualite_df[params_cols].drop_duplicates(subset=['code_parametre']).reset_index(drop=True)
    
    prelevement_cols = ['code_prelevement', 'code_commune', 'date_prelevement', 'nom_uge', 'nom_distributeur', 'nom_moa', 'conclusion_conformite_prelevement', 'conformite_limites_bact_prelevement']
    df_prelevements = mel_qualite_df[prelevement_cols].drop_duplicates(subset=['code_prelevement']).reset_index(drop=True)
    
    mesures_cols = ['code_prelevement', 'code_parametre', 'resultat_numerique', 'resultat_alphanumerique', 'libelle_unite', 'limite_qualite_parametre']
    df_mesures = mel_qualite_df[mesures_cols].drop_duplicates(subset=['code_prelevement', 'code_parametre']).reset_index(drop=True)
    
    communes_reseau_cols = ['code_commune', 'nom_commune', 'code_reseau', 'nom_reseau', 'debut_alim']
    df_communes_reseau = mel_udi_df[communes_reseau_cols].drop_duplicates().reset_index(drop=True)

    print("   -> Nettoyage et normalisation terminés.")
    
    return {
        'parametres': df_parametres,
        'prelevements': df_prelevements,
        'resultats_mesures': df_mesures,
        'communes_reseau': df_communes_reseau
    }


# ----------------------------------------------------------------------
# Orchestrateur Principal
# ----------------------------------------------------------------------

def main_cloud_ready():
    """
    Orchestre le T de l'ETL : Lecture GCS (2 fichiers), Transformation, Écriture GCS (4 tables), Nettoyage GCS.
    """
    if not GCS_BUCKET_NAME or not GCP_PROJECT_ID:
        print("❌ Échec de l'étape de transformation: Les variables d'environnement sont manquantes.")
        sys.exit(1)

    print(f"✅ Liste statique des codes INSEE de la MEL chargée : {len(MEL_COMMUNES_INSEE)} communes.") 

    # Liste pour stocker les noms des fichiers bruts les plus récents (à conserver)
    latest_raw_files = []

    # ------------------------------------------------------
    # 1. Lecture des Données D'ENTRÉE (GCS)
    # ------------------------------------------------------
    try:
        udi_object_name = get_latest_gcs_path(GCS_BUCKET_NAME, "udi_mel")
        latest_raw_files.append(udi_object_name) # <-- Ajout à la liste des fichiers à garder
        df_udi = read_parquet_from_gcs(GCS_BUCKET_NAME, udi_object_name)
        print(f"   ✅ {len(df_udi)} enregistrements UDI bruts chargés.")

        qualite_object_name = get_latest_gcs_path(GCS_BUCKET_NAME, "qualite_eau")
        latest_raw_files.append(qualite_object_name) # <-- Ajout à la liste des fichiers à garder
        df_qualite = read_parquet_from_gcs(GCS_BUCKET_NAME, qualite_object_name)
        print(f"   ✅ {len(df_qualite)} enregistrements de qualité bruts chargés.")
        
    except Exception as e:
        print(f"❌ Échec de la lecture des fichiers bruts depuis GCS : {e}.")
        sys.exit(1)


    # ------------------------------------------------------
    # 2. Transformation et Normalisation
    # ------------------------------------------------------
    try:
        tables_dict = transform_and_normalize_data(df_qualite, df_udi)
        print("✅ Normalisation terminée. 4 tables prêtes pour le chargement.")
    except Exception as e:
        print(f"❌ Échec de la transformation/normalisation : {e}")
        sys.exit(1)
        
    
    # ------------------------------------------------------
    # 3. Écriture des 4 tables de Sortie (GCS/processed)
    # ------------------------------------------------------
    print("\n--- Écriture des 4 tables normalisées vers GCS/processed ---")
    
    for table_name, df in tables_dict.items():
        try:
            save_df_to_gcs(df, GCS_BUCKET_NAME, table_name)
        except Exception as e:
            print(f"❌ Échec critique de l'écriture de la table {table_name}: {e}")
            sys.exit(1)

    # ------------------------------------------------------
    # 4. Nettoyage des anciennes données brutes
    # ------------------------------------------------------
    cleanup_old_gcs_files(GCS_BUCKET_NAME, latest_raw_files)

    print("--- Fin de l'Étape 2: Transformation terminée. ---")


if __name__ == "__main__":
    main_cloud_ready()