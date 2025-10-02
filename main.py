# main.py

import os
import sys

# Importation des fonctions principales des différents modules
# NOTE: Assurez-vous que le nom des fonctions dans les modules est bien 'main_cloud_ready' ou 'main'
# Si vous avez modifié les noms, ajustez l'alias ci-dessous.
try:
    # --------------------------------------------------------------------------
    # 1. Extraction (API -> GCS)
    # Les fonctions doivent lire l'API et sauvegarder le résultat directement sur GCS
    # --------------------------------------------------------------------------
    from src.api.get_resultats_qualite import main_cloud_ready as extract_qualite_eau
    from src.api.liste_communes import main as extract_communes_udi # Assurez-vous d'avoir ce script créé
    
    # --------------------------------------------------------------------------
    # 2. Transformation (GCS/raw -> GCS/processed)
    # Les fonctions doivent lire GCS/raw, transformer, et écrire GCS/processed
    # --------------------------------------------------------------------------
    from src.etl.process_data_liste_communes import main as transform_communes_udi
    from src.etl.process_resultats_qualite import main as transform_qualite_eau
    
    # --------------------------------------------------------------------------
    # 3. Chargement (GCS/processed -> BigQuery)
    # Le script final de chargement
    # --------------------------------------------------------------------------
    from src.load.load_to_bq import main # Assurez-vous d'avoir ce script créé
    load_to_bigquery = main # Alias pour la clarté
    
except ImportError as e:
    print(f"❌ Erreur d'importation. Assurez-vous que tous les fichiers sont présents et les noms des fonctions sont corrects: {e}")
    sys.exit(1)


def run_pipeline():
    """
    Orchestre l'exécution séquentielle du pipeline ETL (Extract, Transform, Load).
    """
    print("🚀 Démarrage du pipeline ETL de l'eau potable sur GitHub Actions...")
    
    # --- 1. EXTRACTION (API -> GCS/raw) ---
    try:
        print("\n--- ÉTAPE 1: Extraction des données API vers GCS (raw) ---")
        extract_communes_udi()
        extract_qualite_eau()
        print("✅ Étape 1 complétée: Données brutes stockées dans GCS.")
    except Exception as e:
        print(f"❌ Échec critique lors de l'extraction: {e}")
        sys.exit(1)

    # --- 2. TRANSFORMATION (GCS/raw -> GCS/processed) ---
    try:
        print("\n--- ÉTAPE 2: Transformation et Normalisation (Filtrage MEL) ---")
        # Le script des communes doit être exécuté en premier pour obtenir la liste des codes INSEE MEL
        transform_communes_udi() 
        # Le script de résultats lit les brutes, utilise la liste MEL, et crée les 4 tables Parquet normalisées
        transform_qualite_eau()
        print("✅ Étape 2 complétée: 4 tables normalisées créées sur GCS (processed).")
    except Exception as e:
        print(f"❌ Échec critique lors de la transformation: {e}")
        sys.exit(1)

    # --- 3. CHARGEMENT (GCS/processed -> BigQuery) ---
    try:
        print("\n--- ÉTAPE 3: Chargement des données dans BigQuery ---")
        # Cette fonction doit lire les 4 fichiers Parquet de GCS/processed 
        # et les charger dans les tables BQ correspondantes.
        load_to_bigquery() 
        print("✅ Étape 3 complétée: BigQuery alimenté.")
    except Exception as e:
        print(f"❌ Échec critique lors du chargement BigQuery: {e}")
        sys.exit(1)

    print("\n🎉 Pipeline ETL exécuté avec succès de bout en bout.")

if __name__ == "__main__":
    run_pipeline()