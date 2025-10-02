# src/etl/process_resultats_qualite.py

import pandas as pd
import sys
import gcsfs 
from config import GCS_BUCKET_NAME, GCP_PROJECT_ID
from typing import Dict, Any, List

# ----------------------------------------------------------------------
# Liste statique des codes INSEE de la MEL (Pour éliminer la dépendance au fichier)
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

# ----------------------------------------------------------------------
# Fonctions utilitaires GCS (Doit être dans un try/except fort)
# ----------------------------------------------------------------------

def get_latest_gcs_file(bucket_name: str, prefix: str, folder: str = "raw") -> str:
    """
    Trouve le chemin GCS complet du fichier Parquet le plus récent dans GCS.
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
        
    target_files.sort(reverse=True)
    
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

def transform_and_normalize_data(raw_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Filtre, nettoie et normalise les données brutes en 4 DataFrames (tables).
    """
    print("   -> Début du nettoyage et de la normalisation...")

    # 1. Nettoyage et Préparation
    raw_df['code_commune'] = raw_df['code_commune'].astype(str).str.zfill(5)
    
    # Filtrage des résultats de qualité pour la MEL (utilise la liste statique)
    mel_df = raw_df[raw_df['code_commune'].isin(MEL_COMMUNES_INSEE)].copy()
    print(f"   -> Enregistrements filtrés pour la MEL : {len(mel_df)}")
    
    if mel_df.empty:
        raise ValueError("Aucun résultat de qualité trouvé pour les communes de la MEL après filtrage.")

    # Définition des clés primaires et étrangères
    mel_df['code_prelevement'] = mel_df['code_prelevement'].astype(str)
    
    # --- 2. Construction de la table PARAMÈTRES (Dimension) ---
    params_cols = [
        'code_parametre', 'libelle_parametre', 'code_type_parametre', 
        'libelle_type_parametre', 'code_parametre_se'
    ]
    df_parametres = mel_df[params_cols].drop_duplicates().reset_index(drop=True)
    
    # --- 3. Construction de la table PRÉLÈVEMENTS (Dimension) ---
    prelevement_cols = [
        'code_prelevement', 'code_commune', 'date_prelevement', 'nom_uge', 
        'nom_distributeur', 'nom_moa', 'conclusion_conformite', 'conformite_limites_bact'
    ]
    df_prelevements = mel_df[prelevement_cols].drop_duplicates(subset=['code_prelevement']).reset_index(drop=True)
    
    # --- 4. Construction de la table RÉSULTATS_MESURES (Fait) ---
    mesures_cols = [
        'code_prelevement', 'code_parametre', 'resultat_numerique', 
        'resultat_alphanumerique', 'libelle_unite', 'limite_qualite_parametre'
    ]
    df_mesures = mel_df[mesures_cols].drop_duplicates(subset=['code_prelevement', 'code_parametre']).reset_index(drop=True)

    # --- 5. Construction de la table COMMUNES_UDI (Dimension) ---
    # Nous utilisons les données d'extraction complète (plus lourdes mais complètes)
    communes_udi_cols = [
        'code_commune', 'code_udi', 'libelle_udi', 'nom_commune',
        'code_service', 'nom_service' 
    ]
    df_communes_udi = mel_df[communes_udi_cols].drop_duplicates().reset_index(drop=True)


    return {
        'parametres': df_parametres,
        'prelevements': df_prelevements,
        'resultats_mesures': df_mesures,
        'communes_udi': df_communes_udi 
    }

# ----------------------------------------------------------------------
# Orchestrateur Principal
# ----------------------------------------------------------------------

def main_cloud_ready():
    """
    Orchestre le T de l'ETL : Lecture GCS, Transformation, Écriture GCS.
    """
    if not GCS_BUCKET_NAME or not GCP_PROJECT_ID:
        print("❌ Échec de l'étape de transformation: GCS_BUCKET_NAME ou GCP_PROJECT_ID sont manquants.")
        sys.exit(1)

    # Initialisation GCSFS (Nécessaire pour le listage de fichiers)
    try:
        gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
    except Exception as e:
        # Nous n'échouons pas ici car le bloc de débogage dans get_udi.py est le plus fiable
        print(f"Avertissement: Initialisation GCSFS dans ETL a rencontré un problème. Détail: {e}")
        
    print(f"✅ Liste statique des codes INSEE de la MEL chargée : {len(MEL_COMMUNES_INSEE)} communes.") 

    # ------------------------------------------------------
    # 1. Lecture du Fichier de Résultats de Qualité Bruts
    # ------------------------------------------------------
    try:
        latest_raw_gcs_path = get_latest_gcs_file(GCS_BUCKET_NAME, "qualite_eau", folder="raw")
        print(f"🔄 Lecture du fichier de résultats bruts le plus récent : {latest_raw_gcs_path}")
        # Pandas lit directement le fichier Parquet depuis GCS
        raw_df_qualite = pd.read_parquet(latest_raw_gcs_path)
        print(f"   ✅ {len(raw_df_qualite)} enregistrements bruts de qualité chargés.")
    except Exception as e:
        print(f"❌ Échec de la lecture du fichier de résultats bruts : {e}.")
        sys.exit(1)


    # ------------------------------------------------------
    # 2. Transformation et Normalisation
    # ------------------------------------------------------
    try:
        tables_dict = transform_and_normalize_data(raw_df_qualite)
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