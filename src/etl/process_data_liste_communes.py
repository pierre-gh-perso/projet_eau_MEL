# src/etl/process_data.py

import pandas as pd
import json
import os

def get_latest_raw_file(directory: str, prefix: str) -> str:
    """
    Trouve le dernier fichier avec un préfixe donné dans un répertoire.

    Args:
        directory (str): Le chemin du répertoire.
        prefix (str): Le préfixe du nom de fichier (ex: "udi_mel").

    Returns:
        str: Le chemin complet du dernier fichier.
    """
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith('.json')]
    if not files:
        raise FileNotFoundError(f"Aucun fichier avec le préfixe '{prefix}' n'a été trouvé dans le dossier '{directory}'.")
        
    # Trie les fichiers par date de modification pour trouver le plus récent
    files.sort(key=lambda f: os.path.getmtime(os.path.join(directory, f)), reverse=True)
    return os.path.join(directory, files[0])

def main():
    """
    Fonction principale pour filtrer les données brutes pour les communes de la MEL.
    """
    # 1. Chargement de la liste des communes de la MEL
    communes_file = 'data/processed/communes_mel.csv'
    if not os.path.exists(communes_file):
        print(f"Erreur : Le fichier des communes de la MEL '{communes_file}' est introuvable.")
        return
    
    communes_df = pd.read_csv(communes_file)
    mel_communes_insee = communes_df['code_insee'].astype(str).unique().tolist()
    print(f"✅ Liste des codes INSEE de la MEL chargée : {len(mel_communes_insee)} communes.")

    # 2. Chargement du dernier fichier de données brutes
    raw_dir = 'data/raw'
    try:
        latest_raw_file = get_latest_raw_file(raw_dir, "udi_mel")
        print(f"🔄 Traitement du fichier brut le plus récent : {latest_raw_file}")
    except FileNotFoundError as e:
        print(f"Erreur : {e}")
        return

    with open(latest_raw_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    print(f"   -> Nombre total d'enregistrements bruts : {len(raw_data)}")
    
    # 3. Filtrage des données
    filtered_data = [
        item for item in raw_data 
        if str(item.get('code_commune')).zfill(5) in mel_communes_insee
    ]
    print(f"✅ Filtrage terminé. Nombre d'enregistrements pour la MEL : {len(filtered_data)}")

    # 4. Sauvegarde des données filtrées
    processed_dir = 'data/processed'
    os.makedirs(processed_dir, exist_ok=True)
    
    output_file = os.path.join(processed_dir, "communes_mel_udi.json")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_data, f, ensure_ascii=False, indent=4)
        
    print(f"✅ Données filtrées sauvegardées dans {output_file}")

if __name__ == "__main__":
    main()