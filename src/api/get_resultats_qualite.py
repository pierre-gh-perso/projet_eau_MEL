# src/api/get_resultats_qualite.py

import requests
import json
import os
from datetime import datetime
import pandas as pd # Reste utile pour la conversion
import pyarrow as pa
import pyarrow.parquet as pq

# URL du point de terminaison pour les résultats d'analyse
BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/"
ENDPOINT = "resultats_dis"

def get_data_from_endpoint_paginated(params: dict = {}) -> list:
    url = f"{BASE_URL}{ENDPOINT}"
    print(f"-> Récupération des données depuis l'endpoint : {url}")

    all_data = []
    page = 1
    total_count = None

    params['size'] = 20000

    while True:
        current_params = params.copy()
        current_params['page'] = page

        try:
            response = requests.get(url, params=current_params)
            response.raise_for_status()

            data = response.json()
            results = data.get('data', [])
            total_count = data.get('count', 0)

            all_data.extend(results)
            print(f"   -> Page {page} récupérée. Enregistrements : {len(results)}. Total récupéré : {len(all_data)} sur {total_count}")

            if len(all_data) >= total_count:
                print("   -> Toutes les données ont été récupérées.")
                break

            page += 1

        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête vers {url}: {e}")
            break

    return all_data

def main():
    raw_dir = 'data/raw'
    os.makedirs(raw_dir, exist_ok=True)

    params = {"code_departement": "59", "nom_distributeur":"ILEO"}

    print("Début du processus de récupération des résultats de qualité de l'eau pour le département du Nord (59).")
    data = get_data_from_endpoint_paginated(params)

    if data:
        # Nom du fichier de sortie avec un horodatage
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(raw_dir, f"qualite_eau_{timestamp}.json")

        # Sauvegarde de la liste de dictionnaires au format JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"✅ Données pour '{ENDPOINT}' sauvegardées dans {output_file}")
        print(f"Total des enregistrements : {len(data)}\n")
    else:
        print("❌ Aucune donnée n'a été récupérée. Vérifiez la connexion ou les paramètres de l'API.")

if __name__ == "__main__":
    main()