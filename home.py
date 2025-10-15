# home.py

import streamlit as st

# Configuration de la page principale (doit être la première commande Streamlit)
st.set_page_config(
    page_title="Accueil - Qualité Eau Potable MEL",
    page_icon="💧",
    layout="wide"
)

# Contenu de la page d'accueil
st.title("💧 Surveillance de la Qualité de l'Eau Potable dans la Métropole Européenne de Lille (MEL)")
st.markdown("""
Bienvenue sur l'application de visualisation des données sur la qualité de l'eau potable. 
Ce projet est alimenté par les données publiques de l'API Hubeau (`qualite_eau_potable`) et automatise l'extraction, la transformation, le chargement (ETL) 
des informations vers **Google Cloud Storage** et **BigQuery** (utilisant uniquement des ressources gratuites).

---

### Objectifs de l'Application

* **Visualiser** les zones géographiques desservies et les résultats des analyses.
* **Identifier** les paramètres d'analyse les plus fréquents et les non-conformités.
* **Fournir** une base pour l'automatisation de la diffusion d'informations (via X).

""")

# Ajoutez une image ou une section sur la méthodologie de collecte des données.
st.info("Veuillez utiliser le menu de navigation à gauche pour explorer les différentes sections.")