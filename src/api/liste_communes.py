# src/api/qualite_eau.py

import requests
import json
import os
from datetime import datetime

#test

# URL de base de l'API Hubeau
BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/"

def get_data_from_endpoint_paginated(endpoint: str, params: dict = {}) -> list:
    """
    Récupère toutes les données d'un point de terminaison de l'API en gérant la pagination.

    Args:
        endpoint (str): Le point de terminaison à appeler (ex: "communes_udi").
        params (dict): Les paramètres de la requête.

    Returns:
        list: Une liste de dictionnaires contenant toutes les données de l'API.
    """
    url = f"{BASE_URL}{endpoint}"
    print(f"-> Récupération des données depuis l'endpoint : {url}")
    
    all_data = []
    page = 1
    total_count = None
    
    # On définit la taille maximale des pages pour réduire le nombre de requêtes
    params['size'] = 20000

    while True:
        current_params = params.copy()
        current_params['page'] = page

        try:
            response = requests.get(url, params=current_params)
            response.raise_for_status() # Lève une exception si le statut est une erreur (4xx ou 5xx)
            
            data = response.json()
            results = data.get('data', [])
            total_count = data.get('count', 0)
            
            all_data.extend(results)
            print(f"   -> Page {page} récupérée. Enregistrements : {len(results)}. Total récupéré : {len(all_data)} sur {total_count}")
            
            # Condition d'arrêt de la boucle
            if len(all_data) >= total_count:
                print("   -> Toutes les données ont été récupérées.")
                break
            
            page += 1

        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête vers {url}: {e}")
            break
            
    return all_data

def main():
    """
    Orchestre l'extraction des données brutes pour les UDI et les sauvegarde.
    """
    raw_dir = 'data/raw'
    os.makedirs(raw_dir, exist_ok=True)

    # Paramètres spécifiques pour l'endpoint communes_udi et le département 59
    endpoint = "communes_udi"
    params = {"code_departement": "59"}
    
    print("Début du processus de récupération des UDI du département du Nord (59).")
    data = get_data_from_endpoint_paginated(endpoint, params)
    
    if data:
        # Nom du fichier de sortie avec un horodatage
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(raw_dir, f"udi_mel_{timestamp}.json")
        
        # Sauvegarde des données brutes dans le dossier raw
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        print(f"✅ Données pour '{endpoint}' sauvegardées dans {output_file}")
        print(f"Total des enregistrements : {len(data)}\n")
    else:
        print("❌ Aucune donnée n'a été récupérée. Vérifiez la connexion ou les paramètres de l'API.")

if __name__ == "__main__":
    main()