# src/etl/process_data_liste_communes.py

import pandas as pd
import os
import sys
# Ajout des imports pour la manipulation Cloud
from io import BytesIO 
import gcsfs # Assurez-vous d'avoir 'pip install gcsfs'
from config import GCS_BUCKET_NAME 

# --- NOUVELLE FONCTION : Chargement dynamique des codes INSEE ---

def load_mel_communes_insee_from_gcs() -> list:
    """
    Charge la liste des codes INSEE de la MEL depuis le fichier CSV stocké sur GCS.
    """
    GCS_CSV_PATH = f"gs://{GCS_BUCKET_NAME}/Geojson/base_villes_mel.csv"
    print(f"🔄 Lecture des codes INSEE depuis GCS : {GCS_CSV_PATH}")

    try:
        # Pandas peut lire directement le CSV depuis GCS si 'gcsfs' est installé et l'authentification est correcte
        df_communes = pd.read_csv(GCS_CSV_PATH)
        
        # Récupération des codes uniques de la colonne "COMMUNE_INSEE"
        insee_codes = df_communes['COMMUNE_INSEE'].astype(str).str.zfill(5).unique().tolist()
        
        if not insee_codes:
            raise ValueError("Le fichier CSV est vide ou la colonne 'COMMUNE_INSEE' ne contient aucune donnée.")
            
        print(f"✅ {len(insee_codes)} codes INSEE uniques chargés depuis GCS.")
        return insee_codes

    except Exception as e:
        print(f"❌ Erreur critique lors du chargement des codes INSEE depuis GCS: {e}")
        # Termine l'exécution si la liste critique ne peut être chargée
        sys.exit(1)


# ----------------------------------------------------------------------
# Remplacement de la liste statique par un placeholder, car elle sera chargée dynamiquement
# ----------------------------------------------------------------------
MEL_COMMUNES_INSEE = [] # La liste sera remplie dans main()


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
    global MEL_COMMUNES_INSEE
    
    if not GCS_BUCKET_NAME or "YOUR_DEFAULT_BUCKET_NAME" in GCS_BUCKET_NAME:
        print("❌ Échec de la transformation : GCS_BUCKET_NAME est mal configuré.")
        sys.exit(1)
        
    # ⭐️ NOUVEAU : Chargement dynamique de la liste des communes avant le reste du traitement
    MEL_COMMUNES_INSEE = load_mel_communes_insee_from_gcs()
    
    print(f"✅ Liste dynamique des codes INSEE de la MEL chargée : {len(MEL_COMMUNES_INSEE)} communes.")

    # 1. Chargement du dernier fichier de données brutes depuis GCS
    try:
        latest_raw_gcs_path = get_latest_gcs_file(GCS_BUCKET_NAME, "udi_mel")
        print(f"🔄 Lecture du fichier brut le plus récent sur GCS : {latest_raw_gcs_path}")
        
        # Pandas lit directement le fichier Parquet depuis GCS
        raw_df = pd.read_parquet(latest_raw_gcs_path)
        print(f"   -> Nombre total d'enregistrements bruts chargés : {len(raw_df)}")

    except FileNotFoundError as e:
        print(f"Erreur : {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Erreur lors de la lecture Parquet depuis GCS: {e}")
        sys.exit(1)


    # 2. Filtrage des données (Utilise la liste fraîchement chargée)
    
    # Assurez-vous que la colonne 'code_commune' est bien au format String 5 chiffres
    raw_df['code_commune'] = raw_df['code_commune'].astype(str).str.zfill(5)
    
    # Filtrage des enregistrements pour les communes de la MEL
    filtered_df = raw_df[raw_df['code_commune'].isin(MEL_COMMUNES_INSEE)].copy()
    
    print(f"✅ Filtrage terminé. Nombre d'enregistrements UDI pour la MEL : {len(filtered_df)}")


    # 3. Sauvegarde de la liste filtrée dans GCS/processed
    
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
    main() # Changement de main_cloud_ready à main car cette fonction n'existe pas dans le script fourni