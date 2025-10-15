# pages_streamlit/1_Carte_Interactive.py

import streamlit as st
import geopandas as gpd
import pandas as pd
from streamlit_folium import st_folium
import folium

# ⭐️ Importation des fonctions de chargement et de requêtage BQ
from src.utils.data_loader import (
    load_mel_geojson_data, 
    load_communes_df_from_geojson, 
    get_latest_results_for_commune 
) 

# =================================================================
#                     APPLICATION STREAMLIT (UI ONLY)
# =================================================================

st.title("🗺️ Carte Interactive des Communes de la MEL")

# --- 1. Chargement des données (Appel des fonctions mises en cache) ---
mel_contours_filtered = load_mel_geojson_data()

if mel_contours_filtered is None:
    st.stop()

# Conversion en DataFrame pour les détails administratifs
df_data = load_communes_df_from_geojson(mel_contours_filtered)

if df_data.empty:
    st.error("❌ Impossible de créer le DataFrame de détails administratifs.")
    st.stop()

st.success(f"✅ Données GeoJSON chargées et prêtes pour {len(mel_contours_filtered['features'])} communes.")


# --- 2. Création et affichage de la carte Folium ---
st.subheader("Visualisation du périmètre des communes")

mel_center = [50.63, 3.06]
m = folium.Map(location=mel_center, zoom_start=10, tiles="cartodb positron")

def style_function(feature):
    return {
        'fillColor': '#3498db',
        'color': 'black',
        'weight': 0.5,
        'fillOpacity': 0.4
    }

# Ajout du GeoJson à la carte
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

# --- 3. Lecture du clic utilisateur et affichage des résultats ---
st.divider()
st.subheader("ℹ️ Détails de la commune sélectionnée")

clicked_feature = map_data.get("last_active_drawing")

if clicked_feature and "properties" in clicked_feature:
    props = clicked_feature["properties"]
    code_insee = str(props.get("code", ""))
    nom_commune = props.get("nom", "N/A")

    st.header(f"Commune : {nom_commune} (Code INSEE : {code_insee})")

    # -----------------------------------------------------
    # ⭐️ AFFICHAGE DES DERNIERS RÉSULTATS DE QUALITÉ (BIGQUERY)
    # -----------------------------------------------------
    
    st.subheader(f"🔬 Résultats de la dernière analyse de qualité de l'eau")

    df_results = get_latest_results_for_commune(code_insee)

    if not df_results.empty:
        # Récupération de la date du prélèvement le plus récent
        last_date = df_results['date_prelevement'].iloc[0].strftime('%d/%m/%Y')
        st.info(f"Base de l'analyse : Résultats du prélèvement effectué le **{last_date}**.")
        
        # Préparation du DataFrame pour l'affichage
        df_results = df_results.rename(columns={
            'libelle_parametre': 'Paramètre',
            'resultat_analyse': 'Résultat',
            'libelle_unite': 'Unité',
            'limite_qualite_reference': 'Limite de Qualité (Référence)',
            'conclusion_conformite': 'Conformité'
        })

        # Mise en forme : combinaison valeur + unité
        df_results['Résultat (Unité)'] = df_results['Résultat'].astype(str) + ' ' + df_results['Unité'].fillna('')
        
        # Mise en valeur de la conformité
        df_results['Conformité'] = df_results['Conformité'].apply(
            lambda x: '✅ Conforme' if x == 'A' else ('❌ Non Conforme' if x == 'B' else '⚠️ NC/Donnée')
        )
        
        # Sélection des colonnes finales
        df_display_results = df_results[[
            'Paramètre',
            'Résultat (Unité)',
            'Limite de Qualité (Référence)',
            'Conformité'
        ]].set_index('Paramètre')
        
        st.dataframe(df_display_results, use_container_width=True)
    else:
        st.warning(f"Aucun résultat d'analyse de qualité de l'eau trouvé dans BigQuery pour {nom_commune}.")

    # -----------------------------------------------------
    # AFFICHAGE DES PROPRIÉTÉS ADMINISTRATIVES (GEOSPATIAL)
    # -----------------------------------------------------
    st.markdown("---")
    st.subheader("🌐 Propriétés géographiques et administratives")
    
    display_columns = ['code', 'nom', 'departement', 'region', 'epci', 'superficie', 'population']
    try:
        selected_row = df_data[df_data["code"] == code_insee].iloc[0]
        props = selected_row.to_dict()

        df_display_geo = pd.DataFrame({
            "Paramètre": [col.capitalize().replace("Epci", "EPCI") for col in display_columns],
            "Valeur": [props.get(col, "N/A") for col in display_columns]
        })
        st.dataframe(df_display_geo.set_index("Paramètre"), use_container_width=True)

    except IndexError:
        st.warning("⚠️ Impossible de trouver les données administratives détaillées.")
        
else:
    st.info("Cliquez sur une commune sur la carte pour afficher ses informations et les résultats de la dernière analyse de qualité de l'eau.")