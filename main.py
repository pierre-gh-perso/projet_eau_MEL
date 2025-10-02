# main.py

import sys
import os
from datetime import datetime

# --- Imports des étapes ETL ---

# Étape 1: Extraction des UDI (Département 59)
from src.api.get_udi import main_cloud_ready as extract_communes_udi

# Étape 1: Extraction des Résultats de Qualité (Département 59)
# NOTE: Vous devez avoir un script similaire appelé 'get_resultats_qualite.py'
# qui récupère les 1.8M de lignes et les sauve dans GCS/raw.
try:
    from src.api.get_resultats_qualite import main as extract_qualite_eau
except ImportError:
    print("Avertissement: Le script 'get_resultats_qualite.py' n'est pas trouvé.")
    sys.exit(1)


# Étape 2: Transformation (Inclut maintenant le filtrage des communes MEL)
from src.etl.process_resultats_qualite import main_cloud_ready as transform_qualite_eau

# Étape 3: Chargement BigQuery
from src.load.load_to_bq import main as load_data_to_bq 


def run_pipeline():
    """
    Orchestre les trois étapes du pipeline ETL : Extraction, Transformation et Chargement.
    """
    start_time = datetime.now()
    print(f"🚀 Démarrage du pipeline ETL de l'eau potable à {start_time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # --- 1. EXTRACTION (API -> GCS/raw) ---
    try:
        print("\n--- ÉTAPE 1: Extraction des données API vers GCS (raw) ---")
        
        # 1a. Extraction des UDI (Liste des UDI par commune du 59)
        extract_communes_udi()      
        
        # 1b. Extraction des résultats de qualité (Les 1.8M de lignes)
        extract_qualite_eau()       
        
        print("✅ Étape 1 complétée: Données brutes stockées dans GCS.")
    except Exception as e:
        print(f"❌ Échec critique lors de l'extraction: {e}")
        # Arrêter le pipeline si l'extraction échoue
        sys.exit(1)


    # --- 2. TRANSFORMATION (GCS/raw -> GCS/processed) ---
    # Cette étape filtre les données pour la MEL et crée les 4 tables normalisées.
    try:
        print("\n--- ÉTAPE 2: Transformation et Normalisation (Filtrage MEL) ---")
        transform_qualite_eau() 
        print("✅ Étape 2 complétée: 4 tables normalisées créées sur GCS (processed).")
    except Exception as e:
        print(f"❌ Échec critique lors de la transformation: {e}")
        sys.exit(1)


    # --- 3. CHARGEMENT (GCS/processed -> BigQuery) ---
    try:
        print("\n--- ÉTAPE 3: Chargement dans BigQuery ---")
        load_data_to_bq()
        print("✅ Étape 3 complétée: Données chargées dans BigQuery.")
    except Exception as e:
        print(f"❌ Échec critique lors du chargement dans BigQuery: {e}")
        sys.exit(1)


    # --- FIN DU PIPELINE ---
    end_time = datetime.now()
    duration = end_time - start_time
    print("\n---------------------------------------------------------")
    print(f"🚀 PIPELINE ETL TERMINÉ avec succès à {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Durée totale d'exécution : {duration}")
    print("---------------------------------------------------------")


if __name__ == "__main__":
    # La fonction principale à exécuter dans l'environnement GitHub Actions
    run_pipeline()