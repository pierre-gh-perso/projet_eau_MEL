import os
import json
import pandas as pd
from google.cloud import storage, bigquery
import pandas_gbq

# =================================================================
#                         CONFIGURATION
# =================================================================
GCS_BUCKET_NAME = "qualite_eau"
GCP_PROJECT_ID = "qualite-eau-473812"

GEOJSON_SOURCE_OBJECT = "Geojson/communes-5m.geojson"
BIGQUERY_TABLE_COMMUNES = "qualite-eau-473812.eau_potable_mel.communes_reseau"
GEOJSON_DEST_OBJECT = "Geojson/communes_filtrees.geojson"

# =================================================================
#                         FONCTIONS CORE OPTIMISÉES
# =================================================================

def load_codes_from_bigquery(table_id):
    """Charge les codes communes depuis la table BigQuery (utilise l'authentification par défaut)."""
    print(f"-> Chargement des codes communes depuis BigQuery: {table_id}")
    query = f"SELECT DISTINCT code_commune FROM `{table_id}`"
    try:
        df_codes = pandas_gbq.read_gbq(query, project_id=GCP_PROJECT_ID) 
        codes_list = df_codes['code_commune'].astype(str).tolist()
        print(f"-> {len(codes_list)} codes uniques récupérés.")
        return set(codes_list)
    except Exception as e:
        print(f"ERREUR: Échec de la lecture BigQuery: {e}")
        return None

def load_geojson_from_gcs(bucket_name, object_name):
    """Charge le fichier GeoJSON brut depuis GCS (utilise l'authentification par défaut)."""
    print(f"-> Chargement du GeoJSON brut depuis GCS: {object_name}")
    try:
        storage_client = storage.Client(project=GCP_PROJECT_ID)
        blob = storage_client.bucket(bucket_name).blob(object_name)
        geojson_data_string = blob.download_as_text()
        return json.loads(geojson_data_string)
    except Exception as e:
        print(f"ERREUR: Échec du chargement GeoJSON GCS: {e}")
        return None

def save_geojson_to_gcs(geojson_data, bucket_name, object_name):
    """Enregistre le fichier GeoJSON filtré sur GCS (utilise l'authentification par défaut)."""
    print(f"-> Enregistrement du GeoJSON filtré vers GCS: {object_name}")
    try:
        storage_client = storage.Client(project=GCP_PROJECT_ID)
        blob = storage_client.bucket(bucket_name).blob(object_name)
        blob.upload_from_string(
            data=json.dumps(geojson_data),
            content_type='application/json'
        )
        print("-> ENREGISTREMENT TERMINÉ avec succès.")
        return True
    except Exception as e:
        print(f"ERREUR: Échec de l'enregistrement GeoJSON GCS: {e}")
        return False

# =================================================================
#                           MAIN LOGIC
# =================================================================

def main():
    # 1. Vérification de l'environnement
    print("Démarrage du pré-traitement GeoJSON...")
        
    # 2. Récupération des codes communes pertinents
    required_codes = load_codes_from_bigquery(BIGQUERY_TABLE_COMMUNES)
    if not required_codes:
        print("Opération annulée car aucun code valide n'a été récupéré.")
        return

    # 3. Chargement du GeoJSON brut
    geojson_brut = load_geojson_from_gcs(GCS_BUCKET_NAME, GEOJSON_SOURCE_OBJECT)
    if not geojson_brut:
        return

    # 4. Filtrage du GeoJSON
    print(f"-> Filtrage de {len(geojson_brut.get('features', []))} features...")
    
    filtered_geojson = {"type": "FeatureCollection", "features": []}
    
    for feature in geojson_brut.get('features', []):
        code = feature.get('properties', {}).get('code', '')
        if str(code) in required_codes:
            filtered_geojson['features'].append(feature)

    print(f"-> Filtrage terminé. {len(filtered_geojson['features'])} features conservées.")
    
    if not filtered_geojson['features']:
        print("AVERTISSEMENT: Le GeoJSON filtré est vide. Vérifiez les codes BigQuery.")
        return

    # 5. Enregistrement du GeoJSON filtré
    save_geojson_to_gcs(filtered_geojson, GCS_BUCKET_NAME, GEOJSON_DEST_OBJECT)

if __name__ == "__main__":
    main()