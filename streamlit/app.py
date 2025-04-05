import streamlit as st
import duckdb
import plotly.express as px
from datetime import datetime
import os

DATABASE_NAME = os.getenv('DATABASE_NAME', 'immobilier_courtage')

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Courtage Immobilier",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_motherduck_connection():
    """Établit une connexion sécurisée à MotherDuck"""
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        st.error("Token d'authentification MotherDuck manquant")
        st.stop()
    
    conn_string = f"md:{DATABASE_NAME}?motherduck_token={token}"
    try:
        return duckdb.connect(conn_string)
    except Exception as e:
        st.error(f"Erreur de connexion : {str(e)}")
        st.stop()

@st.cache_data(ttl=3600)
def load_data(query):
    """Charge les données depuis la base de données avec gestion des erreurs"""
    conn = get_motherduck_connection()
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        st.error(f"Erreur lors de l'exécution de la requête : {str(e)}")
        st.stop()

def clean_taux_data(df):
    """Nettoie les données pour les visualisations"""
    return df[
        (df['categorie_professionnelle'].notna()) &
        (df['categorie_professionnelle'].str.strip() != '') &
        (df['segment_age'] != 'Non défini')
    ].copy()

def main():
    # Configuration de la sidebar
    st.sidebar.title("Filtres")
    st.sidebar.markdown("---")

    # Chargement des données
    with st.spinner("Mise à jour des données..."):
        df_banques = load_data("SELECT * FROM main_gold.metriques_banques")
        df_taux = load_data("SELECT * FROM main_gold.taux_par_profil")
        df_perf = load_data("SELECT * FROM main_gold.performance_source")
        df_conv = load_data("SELECT * FROM main_gold.taux_conversion_opportunites")

    # Nettoyage des données
    df_taux_clean = clean_taux_data(df_taux)

    # En-tête principal
    st.title("📈 Tableau de Bord - Courtage Immobilier")
    st.caption(f"Dernière actualisation : {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    # KPI Principaux
    st.header("Indicateurs Clés")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🏦 Banques Actives", df_banques['partenaire_id'].nunique())
    col2.metric("📄 Propositions", df_banques['nombre_propositions'].sum())
    col3.metric("💶 Taux Moyen", f"{df_banques['taux_moyen_hors_assurance'].mean():.2f}%")
    col4.metric("📈 Taux Conversion", f"{df_conv['taux_conversion'].mean():.2f}%")

    # Visualisations
    tab1, tab2, tab3 = st.tabs(["🏦 Banques", "📊 Analyse Taux", "👤 Profils Clients"])

    with tab1:
        st.subheader("Performance des Banques")
        fig = px.bar(
            df_banques.sort_values('nombre_propositions', ascending=False).head(10),
            x='partenaire_id',
            y='nombre_propositions',
            color='taux_moyen_hors_assurance',
            labels={'partenaire_id': 'Banque', 'nombre_propositions': 'Propositions'},
            color_continuous_scale='Bluered'
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Distribution des Taux d'Intérêt")
        fig = px.box(
            df_taux_clean,
            x='segment_revenus',
            y='taux_moyen_hors_assurance',
            color='segment_endettement',
            labels={
                'segment_revenus': 'Tranche de Revenus',
                'taux_moyen_hors_assurance': 'Taux Hors Assurance'
            }
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("Analyse des Profils Clients")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.treemap(
                df_taux_clean,
                path=[px.Constant("Tous"), 'categorie_professionnelle', 'segment_age'],
                values='nombre_propositions',
                color='taux_moyen_hors_assurance',
                color_continuous_scale='Tealgrn',
                hover_data=['taux_median'],
                labels={'taux_moyen_hors_assurance': 'Taux Moyen'}
            )
            fig.update_traces(
                hovertemplate="<b>%{label}</b><br>Propositions: %{value}<br>Taux médian: %{customdata[0]:.2f}%"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.sunburst(
                df_taux_clean.dropna(subset=['usage_bien', 'type_projet']),
                path=['usage_bien', 'type_projet'],
                values='nombre_propositions',
                color='taux_moyen_hors_assurance',
                color_continuous_scale='Purpor',
                labels={'taux_moyen_hors_assurance': 'Taux Moyen'}
            )
            st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()