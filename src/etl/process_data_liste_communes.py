# src/etl/process_data_liste_communes.py

import pandas as pd
import os
import sys
# Ajout des imports pour la manipulation Cloud
from io import BytesIO 
import gcsfs # Assurez-vous d'avoir 'pip install gcsfs'
from config import GCS_BUCKET_NAME 

# Liste des codes INSEE des communes de la Métropole Européenne de Lille (MEL)
# Cette liste statique est utilisée pour filtrer les données du Nord (59).
# Note : Pour un projet réel, cette liste pourrait être chargée dynamiquement depuis une API.
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


def get_latest_gcs_file(bucket_name: str, prefix: str) -> str:
    """
    Trouve le chemin GCS complet du fichier Parquet le plus récent dans le dossier 'raw'.

    Args:
        bucket_name (str): Le nom du bucket GCS.
        prefix (str): Le préfixe du nom de fichier (ex: "udi_mel").

    Returns:
        str: Le chemin complet 'gs://bucket_name/raw/filename.parquet'.
    """
    # Utiliser gcsfs pour lister les fichiers du bucket
    fs = gcsfs.GCSFileSystem()
    gcs_dir = f"{bucket_name}/raw/"
    
    # Liste tous les fichiers qui correspondent au préfixe dans le dossier 'raw/'
    gcs_files = fs.ls(gcs_dir)
    
    # Filtrer les fichiers Parquet qui commencent par le préfixe
    target_files = [
        f for f in gcs_files 
        if f.startswith(f"{gcs_dir}{prefix}") and f.endswith('.parquet')
    ]
    
    if not target_files:
        # L'erreur que vous avez vue dans le log
        raise FileNotFoundError(f"Aucun fichier avec le préfixe '{prefix}' n'a été trouvé dans le dossier 'gs://{gcs_dir}'.")
        
    # Triez par nom (basé sur l'horodatage dans le nom de fichier) et prenez le plus récent
    target_files.sort(reverse=True)
    
    # Reconstruire le chemin GCS complet
    latest_gcs_path = f"gs://{target_files[0]}"
    return latest_gcs_path


def main():
    """
    Fonction principale pour filtrer les données UDI brutes pour les communes de la MEL,
    lisant depuis GCS et écrivant vers GCS.
    """
    if not GCS_BUCKET_NAME or "YOUR_DEFAULT_BUCKET_NAME" in GCS_BUCKET_NAME:
        print("❌ Échec de la transformation : GCS_BUCKET_NAME est mal configuré.")
        sys.exit(1)

    print(f"✅ Liste statique des codes INSEE de la MEL chargée : {len(MEL_COMMUNES_INSEE)} communes.")

    # 1. Chargement du dernier fichier de données brutes depuis GCS
    try:
        latest_raw_gcs_path = get_latest_gcs_file(GCS_BUCKET_NAME, "udi_mel")
        print(f"🔄 Lecture du fichier brut le plus récent sur GCS : {latest_raw_gcs_path}")
        
        # Pandas lit directement le fichier Parquet depuis GCS
        raw_df = pd.read_parquet(latest_raw_gcs_path)
        print(f"   -> Nombre total d'enregistrements bruts chargés : {len(raw_df)}")

    except FileNotFoundError as e:
        print(f"Erreur : {e}")
        # L'étape d'extraction a échoué à écrire dans GCS ou l'accès est refusé.
        sys.exit(1)
    except Exception as e:
        print(f"Erreur lors de la lecture Parquet depuis GCS: {e}")
        sys.exit(1)


    # 2. Filtrage des données
    
    # Assurez-vous que la colonne 'code_commune' est bien au format String 5 chiffres
    raw_df['code_commune'] = raw_df['code_commune'].astype(str).str.zfill(5)
    
    # Filtrage des enregistrements pour les communes de la MEL
    filtered_df = raw_df[raw_df['code_commune'].isin(MEL_COMMUNES_INSEE)].copy()
    
    print(f"✅ Filtrage terminé. Nombre d'enregistrements UDI pour la MEL : {len(filtered_df)}")


    # 3. Sauvegarde de la liste filtrée dans GCS/processed
    # Ce fichier est essentiel pour l'étape de transformation des résultats
    
    # Le chemin de destination sur GCS
    gcs_processed_path = f"gs://{GCS_BUCKET_NAME}/processed/communes_mel_udi.parquet"
    
    try:
        print(f"🔄 Sauvegarde du fichier UDI filtré vers GCS : {gcs_processed_path}")
        
        # Sauvegarde en Parquet sur GCS
        filtered_df.to_parquet(gcs_processed_path, index=False, engine='pyarrow', compression='snappy')
        
        print(f"✅ Liste UDI MEL sauvegardée dans GCS/processed.")
    
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde du fichier filtré sur GCS : {e}")
        sys.exit(1)


if __name__ == "__main__":
    main_cloud_ready()