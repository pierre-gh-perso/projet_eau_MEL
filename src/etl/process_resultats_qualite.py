# src/etl/process_resultats_qualite.py

import pandas as pd
import os
import json
from datetime import datetime

def get_latest_raw_file(directory: str, prefix: str) -> str:
    """
    Trouve le dernier fichier avec un préfixe donné dans un répertoire.
    """
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith('.json')]
    if not files:
        raise FileNotFoundError(f"Aucun fichier avec le préfixe '{prefix}' n'a été trouvé dans le dossier '{directory}'.")
    
    files.sort(key=lambda f: os.path.getmtime(os.path.join(directory, f)), reverse=True)
    return os.path.join(directory, files[0])

def main():
    """
    Fonction principale pour traiter les données brutes des résultats de qualité de l'eau.
    """
    # 1. Chargement des données brutes
    raw_dir = 'data/raw'
    try:
        latest_raw_file = get_latest_raw_file(raw_dir, "qualite_eau")
        print(f"🔄 Traitement du fichier brut le plus récent : {latest_raw_file}")
    except FileNotFoundError as e:
        print(f"Erreur : {e}")
        return

    with open(latest_raw_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    print(f"   -> Nombre total d'enregistrements bruts : {len(raw_data)}")
    
    # Conversion de la liste de dictionnaires en DataFrame pandas
    df_raw = pd.DataFrame(raw_data)
       
    # 2. Création et traitement des tables
    processed_dir = 'data/processed'
    os.makedirs(processed_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Table 1: prelevements
    # Sélection des colonnes et suppression des doublons
    df_prelevements = df_raw[[
        'code_prelevement', 'date_prelevement', 'code_commune', 'nom_uge', 
        'nom_distributeur', 'nom_moa', 'conclusion_conformite_prelevement',
        'conformite_limites_bact_prelevement', 'conformite_limites_pc_prelevement',
        'conformite_references_bact_prelevement', 'conformite_references_pc_prelevement'
    ]].drop_duplicates(subset='code_prelevement').copy()

    output_file_prelevements = os.path.join(processed_dir, f"prelevements_{timestamp}.parquet")
    df_prelevements.to_parquet(output_file_prelevements, index=False)
    print(f"✅ Table 'prelevements' sauvegardée dans {output_file_prelevements}")
    
    # Table 2: parametres_mesures
    # Sélection des colonnes
    df_parametres_mesures = df_raw[[
        'code_prelevement', 'code_parametre', 'resultat_numerique', 
        'resultat_alphanumerique', 'libelle_unite', 'limite_qualite_parametre',
        'reference_qualite_parametre'
    ]].copy()

    output_file_mesures = os.path.join(processed_dir, f"parametres_mesures_{timestamp}.parquet")
    df_parametres_mesures.to_parquet(output_file_mesures, index=False)
    print(f"✅ Table 'parametres_mesures' sauvegardée dans {output_file_mesures}")

    # Table 3: parametres
    # Sélection des colonnes et suppression des doublons
    df_parametres = df_raw[[
        'code_parametre', 'libelle_parametre', 'code_unite', 'code_type_parametre'
    ]].drop_duplicates(subset='code_parametre').copy()

    output_file_parametres = os.path.join(processed_dir, f"parametres_{timestamp}.parquet")
    df_parametres.to_parquet(output_file_parametres, index=False)
    print(f"✅ Table 'parametres' sauvegardée dans {output_file_parametres}")

if __name__ == "__main__":
    main()