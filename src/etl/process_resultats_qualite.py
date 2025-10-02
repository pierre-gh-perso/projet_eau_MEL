# src/etl/process_resultats_qualite.py

import pandas as pd
import os
import sys
from typing import Dict, Any, List
import gcsfs # Pour interagir avec GCS
from config import GCS_BUCKET_NAME 

# ----------------------------------------------------------------------
# Fonctions utilitaires GCS (Réutilisées de process_data_liste_communes.py)
# ----------------------------------------------------------------------

def get_latest_gcs_file(bucket_name: str, prefix: str, folder: str = "raw") -> str:
    """
    Trouve le chemin GCS complet du fichier Parquet le plus récent.
    """
    fs = gcsfs.GCSFileSystem()
    gcs_dir = f"{bucket_name}/{folder}/"
    
    # Liste tous les fichiers qui correspondent au préfixe
    gcs_files = fs.ls(gcs_dir)
    target_files = [
        f for f in gcs_files 
        if f.startswith(f"{gcs_dir}{prefix}") and f.endswith('.parquet')
    ]
    
    if not target_files:
        raise FileNotFoundError(f"Aucun fichier avec le préfixe '{prefix}' n'a été trouvé dans le dossier 'gs://{gcs_dir}'.")
        
    # Trie par nom (basé sur l'horodatage) et prend le plus récent
    target_files.sort(reverse=True)
    
    # Reconstruire le chemin GCS complet
    latest_gcs_path = f"gs://{target_files[0]}"
    return latest_gcs_path

def save_df_to_gcs(df: pd.DataFrame, bucket_name: str, table_name: str):
    """
    Sauvegarde un DataFrame en Parquet dans le dossier GCS/processed.
    """
    gcs_path = f"gs://{bucket_name}/processed/{table_name}.parquet"
    print(f"   -> Sauvegarde de {len(df)} lignes dans {gcs_path}")
    df.to_parquet(gcs_path, index=False, engine='pyarrow', compression='snappy')
    print(f"   ✅ Table {table_name} sauvegardée.")

# ----------------------------------------------------------------------
# Logique de Transformation et Normalisation
# ----------------------------------------------------------------------

def transform_and_normalize_data(raw_df: pd.DataFrame, mel_communes_insee: List[str]) -> Dict[str, pd.DataFrame]:
    """
    Filtre, nettoie et normalise les données brutes en 4 DataFrames (tables).
    """
    print("   -> Début du nettoyage et de la normalisation...")

    # 1. Nettoyage et Préparation
    # S'assurer que les codes communes sont au bon format (5 chiffres)
    raw_df['code_commune'] = raw_df['code_commune'].astype(str).str.zfill(5)
    
    # Filtrage des résultats de qualité pour la MEL
    mel_df = raw_df[raw_df['code_commune'].isin(mel_communes_insee)].copy()
    print(f"   -> Enregistrements filtrés pour la MEL : {len(mel_df)}")
    
    if mel_df.empty:
        raise ValueError("Aucun résultat de qualité trouvé pour les communes de la MEL après filtrage.")

    # Définition des clés primaires et étrangères
    # Clé Primaire du prélèvement
    mel_df['code_prelevement'] = mel_df['code_prelevement'].astype(str)
    # Clé Primaire du résultat (utilisée pour l'unicité des mesures dans la table des faits)
    mel_df['resultat_id'] = mel_df['code_prelevement'] + mel_df['code_parametre'].astype(str)
    
    # --- 2. Construction de la table PARAMÈTRES (Dimension) ---
    # Récupérer l'information unique sur chaque type de paramètre mesuré
    params_cols = [
        'code_parametre', 'libelle_parametre', 'code_type_parametre', 
        'libelle_type_parametre', 'code_parametre_se'
    ]
    df_parametres = mel_df[params_cols].drop_duplicates().reset_index(drop=True)
    
    # --- 3. Construction de la table PRÉLÈVEMENTS (Fait/Dimension) ---
    # Récupérer l'information unique sur chaque prélèvement
    prelevement_cols = [
        'code_prelevement', 'code_commune', 'date_prelevement', 'nom_uge', 
        'nom_distributeur', 'nom_moa', 'conclusion_conformite', 'conformite_limites_bact'
    ]
    df_prelevements = mel_df[prelevement_cols].drop_duplicates(subset=['code_prelevement']).reset_index(drop=True)
    
    # --- 4. Construction de la table RÉSULTATS_MESURES (Fait) ---
    # La table principale des faits (chaque mesure)
    mesures_cols = [
        'code_prelevement', 'code_parametre', 'resultat_numerique', 
        'resultat_alphanumerique', 'libelle_unite', 'limite_qualite_parametre'
    ]
    # On utilise resultat_id pour garantir l'unicité de la mesure si l'API renvoie des doublons
    df_mesures = mel_df[mesures_cols].drop_duplicates(subset=['code_prelevement', 'code_parametre']).reset_index(drop=True)


    return {
        'parametres': df_parametres,
        'prelevements': df_prelevements,
        'resultats_mesures': df_mesures,
    }

# ----------------------------------------------------------------------
# Orchestrateur Principal
# ----------------------------------------------------------------------

def main():
    """
    Orchestre le T de l'ETL : Lecture GCS, Transformation, Écriture GCS.
    """
    if not GCS_BUCKET_NAME:
        print("❌ Échec de l'étape de transformation: GCS_BUCKET_NAME n'est pas défini.")
        sys.exit(1)

    # ------------------------------------------------------
    # 1. Lecture des Données D'ENTRÉE (GCS)
    # ------------------------------------------------------
    
    # a) Lire la liste des codes INSEE de la MEL (produite à l'étape précédente)
    try:
        udi_mel_gcs_path = f"gs://{GCS_BUCKET_NAME}/processed/communes_mel_udi.parquet"
        print(f"\n🔄 Lecture de la liste INSEE MEL depuis {udi_mel_gcs_path}")
        df_udi_mel = pd.read_parquet(udi_mel_gcs_path, columns=['code_commune'])
        mel_communes_insee = df_udi_mel['code_commune'].astype(str).unique().tolist()
        print(f"   ✅ {len(mel_communes_insee)} codes INSEE de la MEL chargés.")
    except Exception as e:
        print(f"❌ Échec de la lecture de la liste INSEE MEL : {e}. L'étape 2 s'arrête.")
        sys.exit(1)
        
    # b) Lire le dernier fichier de résultats de qualité bruts
    try:
        latest_raw_gcs_path = get_latest_gcs_file(GCS_BUCKET_NAME, "qualite_eau", folder="raw")
        print(f"🔄 Lecture du fichier de résultats bruts le plus récent : {latest_raw_gcs_path}")
        raw_df_qualite = pd.read_parquet(latest_raw_gcs_path)
        print(f"   ✅ {len(raw_df_qualite)} enregistrements bruts de qualité chargés.")
    except Exception as e:
        print(f"❌ Échec de la lecture du fichier de résultats bruts : {e}.")
        sys.exit(1)


    # ------------------------------------------------------
    # 2. Transformation et Normalisation
    # ------------------------------------------------------
    try:
        tables_dict = transform_and_normalize_data(raw_df_qualite, mel_communes_insee)
        
        # Ajout de la table COMMUNES_UDI filtrée, qui a été produite à l'étape précédente
        # On lit le fichier communes_mel_udi.parquet et on le charge comme table dimension
        df_communes_udi = pd.read_parquet(udi_mel_gcs_path)
        df_communes_udi = df_communes_udi.rename(columns={'code_commune_insee': 'code_commune'})
        tables_dict['communes_udi'] = df_communes_udi
        
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

    print("--- Fin de l'Étape 2: Transformation terminée. ---")


if __name__ == "__main__":
    main_cloud_ready()