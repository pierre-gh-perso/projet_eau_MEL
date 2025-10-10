# =================================================================
#              APPLICATION STREAMLIT INTERACTIVE (FOLIUM)
# =================================================================

import streamlit as st
import geopandas as gpd
import pandas as pd
import json
import os
from google.cloud import storage
from streamlit_folium import st_folium
import folium

# =================================================================
#                         CONFIGURATION
# =================================================================

GCS_BUCKET_NAME = "qualite_eau"
GCP_PROJECT_ID = "qualite-eau-473812"
GCP_KEY_FILE_PATH = r"D:\Pierre\qualite_eau_mel\data\qualite-eau-473812-09ac7a86b2ab.json"

GEOJSON_OBJECT_NAME = "Geojson/communes_filtrees.geojson"
DEPARTMENT_CODE_TO_FILTER = 59

# =================================================================
#                      FONCTIONS TECHNIQUES
# =================================================================

@st.cache_data(ttl=3600)
def load_geojson_from_gcs_native(bucket_name, object_name, credentials_path):
    """Charge le fichier GeoJSON depuis Google Cloud Storage."""
    if not os.path.exists(credentials_path):
        st.error(f"❌ Erreur: Clé JSON non trouvée à l'emplacement spécifié : {credentials_path}")
        return None
    try:
        storage_client = storage.Client.from_service_account_json(credentials_path, project=GCP_PROJECT_ID)
        blob = storage_client.bucket(bucket_name).blob(object_name)
        geojson_data_string = blob.download_as_text()
        return json.loads(geojson_data_string)
    except Exception as e:
        st.error(f"❌ Échec de la lecture GCS/GeoJSON : {e}")
        return None


def filter_geojson_by_department(geojson_data, department_code):
    """Filtre les features GeoJSON par code département."""
    if not geojson_data or 'features' not in geojson_data:
        return None
    filtered_geojson = {"type": "FeatureCollection", "features": []}
    dep_code_str = str(department_code)
    for feature in geojson_data['features']:
        dep_property = str(feature.get('properties', {}).get('departement', ''))
        if dep_property == dep_code_str:
            filtered_geojson['features'].append(feature)
    return filtered_geojson


# =================================================================
#                    APPLICATION STREAMLIT
# =================================================================

st.set_page_config(layout="wide")
st.title("💧 Qualité de l'Eau - Carte Interactive MEL (Folium)")

# --- 1. Chargement des données ---
geojson_brut = load_geojson_from_gcs_native(GCS_BUCKET_NAME, GEOJSON_OBJECT_NAME, GCP_KEY_FILE_PATH)
if geojson_brut is None:
    st.stop()

mel_contours_filtered = filter_geojson_by_department(geojson_brut, DEPARTMENT_CODE_TO_FILTER)

if not mel_contours_filtered or not mel_contours_filtered['features']:
    st.error("❌ Le filtrage n'a retourné aucune commune. Vérifiez le code département ou les données.")
    st.stop()

st.success(f"✅ Données GeoJSON chargées et filtrées pour {len(mel_contours_filtered['features'])} communes.")

# --- 2. Conversion en GeoDataFrame ---
gdf_communes = gpd.GeoDataFrame.from_features(mel_contours_filtered["features"])
df_data = gdf_communes.drop(columns="geometry").copy()
df_data["code"] = df_data["code"].astype(str)

# --- 3. Création de la carte Folium ---
st.subheader("🗺️ Carte interactive des communes du département 59")

# Définir le centre (coordonnées approximatives de la MEL)
mel_center = [50.63, 3.06]
m = folium.Map(location=mel_center, zoom_start=10, tiles="cartodb positron")

# Ajouter les polygones du GeoJSON
def style_function(feature):
    return {
        'fillColor': '#3498db',
        'color': 'black',
        'weight': 0.5,
        'fillOpacity': 0.4
    }

# Popup = nom + code INSEE
folium.GeoJson(
    mel_contours_filtered,
    name="Communes",
    style_function=style_function,
    highlight_function=lambda x: {'fillColor': '#f1c40f', 'fillOpacity': 0.7},
    tooltip=folium.GeoJsonTooltip(
        fields=['nom', 'code'],
        aliases=['Commune', 'Code INSEE'],
        localize=True
    )
).add_to(m)

# Affichage de la carte et capture du clic
map_data = st_folium(m, width=900, height=600)

# --- 4. Lecture du clic utilisateur ---
st.divider()
st.subheader("ℹ️ Détails de la commune sélectionnée")

clicked_feature = map_data.get("last_active_drawing")

if clicked_feature and "properties" in clicked_feature:
    props = clicked_feature["properties"]
    code_insee = str(props.get("code", ""))
    nom_commune = props.get("nom", "N/A")

    st.success(f"✅ Commune sélectionnée : **{nom_commune}** (Code INSEE : {code_insee})")

    # --- 5. Affichage du tableau de détails ---
    display_columns = ['code', 'nom', 'departement', 'region', 'epci', 'superficie', 'population']
    try:
        selected_row = df_data[df_data["code"] == code_insee].iloc[0]
        props = selected_row.to_dict()

        df_display = pd.DataFrame({
            "Paramètre": [col.capitalize().replace("Epci", "EPCI") for col in display_columns],
            "Valeur": [props.get(col, "N/A") for col in display_columns]
        })
        st.dataframe(df_display.set_index("Paramètre"), use_container_width=True)

    except IndexError:
        st.warning("⚠️ Impossible de trouver les données détaillées pour cette commune.")
else:
    st.info("Cliquez sur une commune sur la carte pour afficher ses informations ci-dessous.")
