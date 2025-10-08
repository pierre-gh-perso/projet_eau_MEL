# src/etl/process_resultats_qualite.py

import pandas as pd
import sys
import gcsfs 
import os
import io
from google.cloud import storage 
from config import GCS_BUCKET_NAME, GCP_PROJECT_ID
from typing import Dict, Any, List, Set
from datetime import datetime

CRITERE_MOA_MEL = "MEL - MÉTROPOLE EUROP. DE LILLE"

# Initialisation du client GCS
storage_client = storage.Client()

# ----------------------------------------------------------------------
# Fonctions utilitaires GCS
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

    # Convertir la liste en ensemble pour une recherche rapide
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
# Filtrage des Codes Communes par Critère MOA
# ----------------------------------------------------------------------

def get_commune_codes_from_moa(df: pd.DataFrame, moa_critere: str) -> Set[str]:
    """
    Filtre le DataFrame (qualité ou UDI) pour les enregistrements correspondant 
    au critère de Maîtrise d'Ouvrage et retourne l'ensemble des codes communes uniques.
    """
    print(f"⚙️ Recherche des codes communes pour MoA: '{moa_critere}'")
    
    # 1. Normalisation pour une recherche robuste (minuscules, suppression espaces)
    moa_critere_normalise = moa_critere.strip().lower()

    # 2. Application du filtre dans Pandas
    df['nom_moa_clean'] = df['nom_moa'].astype(str).str.strip().str.lower()
    
    # 3. Filtrage du DataFrame
    df_filtre = df[df['nom_moa_clean'] == moa_critere_normalise]
    
    # 4. Extraction des codes communes uniques
    codes_insee_set = set(df_filtre['code_commune'].astype(str).str.zfill(5))
    
    print(f"   ✅ {len(codes_insee_set)} codes communes uniques trouvés via MoA.")
    
    # 5. Nettoyage de la colonne temporaire
    df.drop(columns=['nom_moa_clean'], inplace=True, errors='ignore')
    
    return codes_insee_set

# ----------------------------------------------------------------------
# Logique de Transformation et Normalisation 
# ----------------------------------------------------------------------

def transform_and_normalize_data(df_qualite: pd.DataFrame, df_udi: pd.DataFrame, target_insee_codes: Set[str]) -> Dict[str, pd.DataFrame]:
    """
    Filtre, nettoie et normalise les données brutes en tables finales.
    """
    print("   -> Début du nettoyage et de la normalisation...")
    if not target_insee_codes:
        raise ValueError("La liste des codes INSEE cibles est vide. Arrêt du traitement.")

    # 1. Préparation et FILTRAGE des Résultats de Qualité (Table de faits principale)
    df_qualite['code_commune'] = df_qualite['code_commune'].astype(str).str.zfill(5)
    
    mel_qualite_df = df_qualite[df_qualite['code_commune'].isin(target_insee_codes)].copy()
    print(f"   -> Enregistrements filtrés pour la MEL : {len(mel_qualite_df)}")
    
    if mel_qualite_df.empty:
        raise ValueError("Aucun résultat de qualité trouvé pour les communes de la MEL après filtrage.")
        
    mel_qualite_df['code_prelevement'] = mel_qualite_df['code_prelevement'].astype(str)
    mel_qualite_df['code_parametre'] = mel_qualite_df['code_parametre'].astype(str)

    # 2. FILTRAGE du DF UDI (pour les infos de réseau/commune)
    df_udi['code_commune'] = df_udi['code_commune'].astype(str).str.zfill(5)
    mel_udi_df = df_udi[df_udi['code_commune'].isin(target_insee_codes)].copy()

    # --- Construction des 4 tables ---
    params_cols = ['code_parametre', 'libelle_parametre', 'code_type_parametre', 'code_parametre_se', 'libelle_parametre_maj', 'libelle_unite', 'limite_qualite_parametre'] 
    df_parametres = mel_qualite_df[params_cols].drop_duplicates(subset=['code_parametre']).reset_index(drop=True)
    
    prelevement_cols = ['code_prelevement', 'code_commune', 'date_prelevement', 'conclusion_conformite_prelevement', 'conformite_limites_bact_prelevement']
    df_prelevements = mel_qualite_df[prelevement_cols].drop_duplicates(subset=['code_prelevement']).reset_index(drop=True)
    
    mesures_cols = ['code_prelevement', 'code_parametre', 'resultat_numerique', 'resultat_alphanumerique']
    df_mesures = mel_qualite_df[mesures_cols].drop_duplicates(subset=['code_prelevement', 'code_parametre']).reset_index(drop=True)
    
    # Jointure pour obtenir les infos de réseau et commune
    udi_dim_cols = ['code_commune', 'nom_commune', 'code_reseau', 'nom_reseau', 'debut_alim']
    df_communes_udi = mel_udi_df[udi_dim_cols].drop_duplicates(subset=['code_commune']).set_index('code_commune')
    qualite_dim_cols = ['code_commune', 'nom_distributeur', 'nom_uge', 'nom_moa']
    df_org_info = mel_qualite_df[qualite_dim_cols].drop_duplicates().set_index('code_commune')
    df_communes_reseau = df_communes_udi.merge(
        df_org_info,
        left_index=True,
        right_index=True,
        how='left'
    ).reset_index()
    communes_reseau_cols = ['code_commune', 'nom_commune', 'code_reseau', 'nom_reseau', 'nom_distributeur', 'nom_uge', 'nom_moa', 'debut_alim']
    df_communes_reseau = df_communes_reseau[communes_reseau_cols].drop_duplicates().reset_index(drop=True)

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
    Orchestre le T de l'ETL : Lecture GCS (2 fichiers), Détermination des Codes MEL, 
    Transformation, Écriture GCS (4 tables), Nettoyage GCS.
    """
    if not GCS_BUCKET_NAME or not GCP_PROJECT_ID:
        print("❌ Échec de l'étape de transformation: Les variables d'environnement sont manquantes.")
        sys.exit(1)

    latest_raw_files = []
    df_udi, df_qualite = None, None

    # ------------------------------------------------------
    # 1. Lecture des Données D'ENTRÉE (GCS)
    # ------------------------------------------------------
    try:
        udi_object_name = get_latest_gcs_path(GCS_BUCKET_NAME, "udi_mel")
        latest_raw_files.append(udi_object_name)
        df_udi = read_parquet_from_gcs(GCS_BUCKET_NAME, udi_object_name)
        print(f"   ✅ {len(df_udi)} enregistrements UDI bruts chargés.")

        qualite_object_name = get_latest_gcs_path(GCS_BUCKET_NAME, "qualite_eau")
        latest_raw_files.append(qualite_object_name)
        df_qualite = read_parquet_from_gcs(GCS_BUCKET_NAME, qualite_object_name)
        print(f"   ✅ {len(df_qualite)} enregistrements de qualité bruts chargés.")
        
    except Exception as e:
        print(f"❌ Échec de la lecture des fichiers bruts depuis GCS : {e}.")
        sys.exit(1)

    # ------------------------------------------------------
    # 2. DÉTERMINATION DYNAMIQUE DES CODES COMMUNES
    # ------------------------------------------------------
    try:
        mel_codes_insee = get_commune_codes_from_moa(df_qualite, CRITERE_MOA_MEL)
        
        if not mel_codes_insee:
            raise ValueError("Le filtrage par MoA n'a retourné aucun code commune.")
            
        print(f"✅ Codes INSEE de la MEL déterminés dynamiquement : {len(mel_codes_insee)} communes.")
        
    except Exception as e:
        print(f"❌ Échec de la détermination des codes MEL : {e}")
        sys.exit(1)

    # ------------------------------------------------------
    # 3. Transformation et Normalisation
    # ------------------------------------------------------
    try:
        # ⚠️ CORRECTION : Passer mel_codes_insee en argument
        tables_dict = transform_and_normalize_data(df_qualite, df_udi, mel_codes_insee) 
        print("✅ Normalisation terminée. 4 tables prêtes pour le chargement.")
    except Exception as e:
        print(f"❌ Échec de la transformation/normalisation : {e}")
        sys.exit(1)
            
    
    # ------------------------------------------------------
    # 4. Écriture des 4 tables de Sortie (GCS/processed)
    # ------------------------------------------------------
    print("\n--- Écriture des 4 tables normalisées vers GCS/processed ---")
    
    for table_name, df in tables_dict.items():
        try:
            save_df_to_gcs(df, GCS_BUCKET_NAME, table_name)
        except Exception as e:
            print(f"❌ Échec critique de l'écriture de la table {table_name}: {e}")
            sys.exit(1)

    # ------------------------------------------------------
    # 5. Nettoyage des anciennes données brutes
    # ------------------------------------------------------
    cleanup_old_gcs_files(GCS_BUCKET_NAME, latest_raw_files)

    print("--- Fin de l'Étape 2: Transformation terminée. ---")


if __name__ == "__main__":
    main_cloud_ready()