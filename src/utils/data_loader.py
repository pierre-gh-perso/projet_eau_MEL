# src/utils/data_loader.py (VERSION ADAPTÉE AU DÉPLOIEMENT STREAMLIT CLOUD)

import streamlit as st
import json
import geopandas as gpd
import pandas as pd
import os
from google.cloud import storage, bigquery
from typing import Dict, Any, List

# =================================================================
# 1. CONFIGURATION (Utilisation des secrets Streamlit)
# =================================================================
# Les variables critiques sont lues directement depuis st.secrets

# Clés de configuration générales
GCP_PROJECT_ID = "qualite-eau-473812"
GCS_BUCKET_NAME = "qualite_eau"
GEOJSON_OBJECT_NAME = "Geojson/communes_filtrees.geojson"
BIGQUERY_DATASET_ID = "eau_potable_mel"
# Le chemin GCP_KEY_FILE_PATH est retiré

# =================================================================
# 2. CLIENTS (Authentification par secrets Streamlit)
# =================================================================

@st.cache_resource
def get_service_account_info():
    """Récupère les informations du compte de service depuis st.secrets."""
    try:
        # Assurez-vous que la structure de vos secrets est 'gcp_service_account'
        # et qu'elle contient les clés nécessaires.
        return st.secrets["gcp_service_account"]
    except KeyError:
        st.error("❌ Erreur de configuration: La clé 'gcp_service_account' est manquante dans les secrets Streamlit.")
        return None

@st.cache_resource
def get_gcs_storage_client():
    """Crée et met en cache le client Google Cloud Storage à partir des secrets."""
    sa_info = get_service_account_info()
    if sa_info is None:
        return None
    try:
        # Utilise from_service_account_info pour l'authentification
        return storage.Client.from_service_account_info(sa_info, project=GCP_PROJECT_ID)
    except Exception as e:
        st.error(f"❌ Échec de l'initialisation du client GCS par secrets : {e}")
        return None

@st.cache_resource
def get_bigquery_client():
    """Crée et met en cache le client Google Cloud BigQuery à partir des secrets."""
    sa_info = get_service_account_info()
    if sa_info is None:
        return None
    try:
        # Utilise from_service_account_info pour l'authentification
        return bigquery.Client.from_service_account_info(sa_info, project=GCP_PROJECT_ID)
    except Exception as e:
        st.error(f"❌ Échec de l'initialisation du client BigQuery par secrets : {e}")
        return None

# =================================================================
# 3. FONCTIONS DE CHARGEMENT DE DONNÉES (Utilisation de st.cache_data)
# =================================================================

@st.cache_data(ttl=3600)
def load_mel_geojson_data() -> Dict[str, Any] | None:
    """Charge le fichier GeoJSON filtré (communes de la MEL) depuis GCS."""
    
    storage_client = get_gcs_storage_client()
    if storage_client is None:
        return None
        
    try:
        with st.spinner(f"Chargement des contours GeoJSON de la MEL depuis GCS..."):
            blob = storage_client.bucket(GCS_BUCKET_NAME).blob(GEOJSON_OBJECT_NAME)
            geojson_data_string = blob.download_as_text()
            geojson_data = json.loads(geojson_data_string)
            
            if not geojson_data or not geojson_data.get('features'):
                 st.error("❌ Le GeoJSON est vide ou mal formaté.")
                 return None

            return geojson_data
            
    except Exception as e:
        st.error(f"❌ Échec de la lecture GCS/GeoJSON : {e}")
        return None

@st.cache_data(ttl=300)
def get_latest_results_for_commune(code_insee: str) -> pd.DataFrame:
    """
    Récupère l'ensemble des résultats de mesure pour la dernière date de prélèvement
    disponible pour la commune spécifiée, en interrogeant BigQuery.
    """
    
    client = get_bigquery_client()
    
    # --------------------------------------------------------------------------
    # REQUÊTE BIGQUERY : Trouve le prélèvement le plus récent et toutes ses mesures.
    # --------------------------------------------------------------------------
    query = f"""
    WITH LatestPrelevement AS (
        -- 1. Trouver le code de prélèvement le plus récent (MAX(date_prelevement)
        SELECT
            code_prelevement,
            date_prelevement
        FROM
            `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.prelevements`
        WHERE
            code_commune = '{code_insee}'
        ORDER BY
            date_prelevement DESC
        LIMIT 1
    )
    SELECT
        t2.libelle_parametre,
        t1.resultat_analyse,
        t1.code_unite,
        t3.libelle_unite,
        t1.limite_qualite_reference,
        t1.conclusion_conformite,
        lp.date_prelevement -- Date pour l'affichage
    FROM
        `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.resultats_mesures` AS t1
    INNER JOIN
        LatestPrelevement AS lp
        ON t1.code_prelevement = lp.code_prelevement
    LEFT JOIN
        `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.parametres` AS t2
        ON t1.code_parametre = t2.code_parametre
    LEFT JOIN
        `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.unites` AS t3
        ON t1.code_unite = t3.code_unite
    """
    
    try:
        with st.spinner(f"Interrogation BigQuery pour le code {code_insee}..."):
            df = client.query(query).to_dataframe()
            return df
    except Exception as e:
        st.error(f"❌ Erreur lors de l'interrogation BQ pour les résultats : {e}")
        return pd.DataFrame()
        
# ----------------- FONCTIONS AUXILIAIRES POUR LA CARTE -----------------

@st.cache_data(ttl=3600)
def load_communes_df_from_geojson(geojson_data: Dict[str, Any]) -> pd.DataFrame:
    """Crée un DataFrame Pandas (non-géo) pour l'affichage des détails administratifs."""
    if not geojson_data or 'features' not in geojson_data:
        return pd.DataFrame()
        
    gdf_communes = gpd.GeoDataFrame.from_features(geojson_data["features"])
    df_data = gdf_communes.drop(columns="geometry").copy()
    df_data["code"] = df_data["code"].astype(str)
    return df_data