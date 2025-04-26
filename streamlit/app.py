import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import sys
from streamlit_extras.app_logo import add_logo
from config import COLORS, PLOT_CONFIG, TEXTS
# Ajout du chemin racine au path pour pouvoir importer utils et config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DATABASE_NAME = os.getenv('DATABASE_NAME', 'immobilier_courtage')

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Courtage Immobilier",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Chargement des styles CSS personnalisés
with open("assets/css/style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


@st.cache_resource
def get_connect_to_motherduck():
    """
    Établit une connexion à MotherDuck et attache la base de données
    """
    try:
        # Connexion à DuckDB en mode MotherDuck
        conn = duckdb.connect(f"md:{DATABASE_NAME}", read_only=False)
        return conn
    except Exception as e:
        raise


# Fonction générique pour charger les données
@st.cache_data(ttl=3600)
def load_data(query, periode=None, date_column=None):
    """Charge les données depuis la base avec un filtre de période facultatif"""
    conn = get_connect_to_motherduck()
    if periode and date_column:
        date_limite = (datetime.now() - timedelta(days=periode)).strftime('%Y-%m-%d')
        query += f" WHERE {date_column} >= '{date_limite}'"
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {e}")
        st.stop()

# Chargement des différentes métriques
def load_metrics_banques():
    return load_data("SELECT * FROM main_gold.metriques_banques")

def load_conversion_opportunites(periode):
    return load_data("SELECT * FROM main_gold.taux_conversion_opportunites")

def load_taux_profil():
    return load_data("SELECT * FROM main_gold.taux_par_profil")

def load_performance_source(periode):
    return load_data("SELECT * FROM main_gold.performance_source", periode, "mois_acquisition")

# Interface utilisateur Streamlit
def main():
    
    #st.sidebar.image("../images/luffy.jpg")

    # Sidebar pour les filtres
    st.sidebar.title("Filtres")
    
    # Filtre de période
    periode_options = {
        "Dernier mois": 30,
        "Dernier trimestre": 90,
        "Dernière année": 365,
        "Toutes les données": None
    }
    periode_selectionnee = st.sidebar.selectbox(
        "Période d'analyse",
        options=list(periode_options.keys()),
        index=2  # Par défaut: dernière année
    )
    periode = periode_options[periode_selectionnee]
    
    # Filtres additionnels
    with st.sidebar.expander("Filtres avancés"):
        # Filtre pour le segment client
        segment_age_options = ["Tous", "Jeune", "Milieu de vie", "Senior"]
        segment_age = st.selectbox("Segment d'âge", segment_age_options, index=0)
        
        segment_revenus_options = ["Tous", "Revenus modestes", "Revenus moyens", "Revenus élevés"]
        segment_revenus = st.selectbox("Segment de revenus", segment_revenus_options, index=0)
        
        usage_bien_options = ["Tous", "Résidence principale", "Investissement locatif"]
        usage_bien = st.selectbox("Usage du bien", usage_bien_options, index=0)
    
    # Chargement des données avec indicateurs de chargement
    with st.spinner("Chargement des données..."):
        df_banques = load_metrics_banques()
        df_conversion = load_conversion_opportunites(periode)
        df_taux_profil = load_taux_profil()
        df_perf_source = load_performance_source(periode)
        
        # Application des filtres avancés (si nécessaire)
        if segment_age != "Tous" and "segment_age" in df_taux_profil.columns:
            df_taux_profil = df_taux_profil[df_taux_profil["segment_age"] == segment_age]
        
        if segment_revenus != "Tous" and "segment_revenus" in df_taux_profil.columns:
            df_taux_profil = df_taux_profil[df_taux_profil["segment_revenus"] == segment_revenus]
            
        if usage_bien != "Tous" and "usage_bien" in df_taux_profil.columns:
            df_taux_profil = df_taux_profil[df_taux_profil["usage_bien"] == usage_bien]
    
    # En-tête de la page
    st.title("📈 Dashboard Courtage Immobilier : France")
    st.markdown(f"*Données à jour au {datetime.now().strftime('%d/%m/%Y')}*")
    
    # KPIs principaux
    st.header("Indicateurs clés de performance")
    col1, col2, col3, col4 = st.columns(4)
    
    total_opportunites = df_perf_source["nombre_opportunites"].count() if not df_perf_source.empty else 0
    total_converties = df_perf_source["nombre_converties"].sum() if not df_perf_source.empty else 0
    #taux_conversion_global = (total_converties / total_opportunites * 100) if total_opportunites > 0 else 0
    
    #col1.metric("Nombre d'opportunités", f"{total_opportunites:,}".replace(",", " "))
    col1.metric("🏦 Nombre de banques", df_banques['partenaire_id'].nunique())
    col2.metric("💰 Nombre de propositions", f"{df_banques['nombre_propositions'].sum():,}".replace(",", " ") if not df_banques.empty else 0)
    #col3.metric("☁️ Taux de conversion", f"{taux_conversion_global:.1f}%")
    col3.metric("☁️ Montant moyen du prêt", f"{df_banques['montant_moyen'].mean():,.0f} €")
    col4.metric("🌟 Taux moyen hors assurance", f"{df_banques['taux_moyen_hors_assurance'].mean():.2f}%" if not df_banques.empty else "0.00%")
    
    # Aperçu des données filtrées
    with st.expander("Aperçu des données"):
        st.dataframe(df_banques.head(10), use_container_width=True)

    # --------- SECTION 2: ANALYSE DES  PROPOSITION TES PAR SEGMENT D'AGE ---------
    st.header("Analyse des propositions par segment d'age")
    # Calcul des ventes par catégorie
    proposition_per_age = df_taux_profil.groupby("segment_age")["nombre_propositions"].sum().reset_index()
    proposition_per_age = proposition_per_age.rename(columns={"nombre_propositions": "total_propositions"})
    proposition_per_age = proposition_per_age.sort_values("total_propositions", ascending=False)


    # Visualisation des ventes par catégorie avec un graphique en barres
    fig1 = px.bar(
        proposition_per_age,
        x="segment_age",
        y="total_propositions",
        color="segment_age",
        labels={"total_propositions": "Propositions", "segment_age": "Age"},
        template=PLOT_CONFIG["template"],
        color_discrete_sequence=PLOT_CONFIG["color_discrete_sequence"],
    )

    fig1.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=20, b=30),
        showlegend=False,
        xaxis_title="",
        yaxis_title="Propositions",
    )

    st.plotly_chart(fig1, use_container_width=True)
    
    
    if not df_banques.empty:
        # Tri des banques par nombre de propositions                
    
        st.subheader("Répartition des durées de prêt")
        duree_data = df_banques[[
            'count_duree_15ans_ou_moins',
            'count_duree_15_20ans',
            'count_duree_20_25ans',
            'count_duree_plus_25ans']].sum().reset_index()

        duree_data.columns = ['Durée', 'Nombre']
        duree_data['Durée'] = duree_data['Durée'].str.replace('count_duree_', '').str.replace('_', ' ').str.replace('ou moins', '≤ 15 ans').str.replace('plus 25ans', '> 25 ans')

        fig_duree = px.bar(
            duree_data,
            x='Durée',
            y='Nombre',
            title="Répartition globale des durées de prêt",
            labels={"Durée": "Durée du prêt", "Nombre": "Nombre de prêts"}
        )
        st.plotly_chart(fig_duree, use_container_width=True)


    
    # Profil des clients
    st.header("Analyse des profils clients")
    
    if not df_taux_profil.empty:
        # Regroupement par segment d'âge
        tabs_profil = st.tabs(["Segment d'âge", "Niveau de revenus", "Situation professionnelle", "Type de projet"])
        
        with tabs_profil[0]:
            if "segment_age" in df_taux_profil.columns:
                age_data = df_taux_profil.groupby('segment_age').agg({
                    'nombre_propositions': 'sum',
                    'taux_moyen_hors_assurance': 'mean'
                }).reset_index()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Répartition des propositions par âge
                    fig_age_pie = px.pie(
                        age_data,
                        names='segment_age',
                        values='nombre_propositions',
                        title="Répartition des propositions par segment d'âge",
                        hole=0.4
                    )
                    fig_age_pie.update_traces(textinfo='percent+label')
                    st.plotly_chart(fig_age_pie, use_container_width=True)
                
                with col2:
                    # Taux moyens par segment d'âge
                    fig_age_taux = px.bar(
                        age_data,
                        x='segment_age',
                        y='taux_moyen_hors_assurance',
                        title="Taux moyen par segment d'âge",
                        labels={'segment_age': "Segment d'âge", 'taux_moyen_hors_assurance': 'Taux moyen (%)'},
                        color='taux_moyen_hors_assurance',
                        color_continuous_scale=px.colors.sequential.Blues,
                        text_auto='.2f'
                    )
                    fig_age_taux.update_traces(texttemplate='%{text}%', textposition='outside')
                    st.plotly_chart(fig_age_taux, use_container_width=True)
        
        with tabs_profil[1]:
            if "segment_revenus" in df_taux_profil.columns:
                revenus_data = df_taux_profil.groupby('segment_revenus').agg({
                    'nombre_propositions': 'sum',
                    'taux_moyen_hors_assurance': 'mean'
                }).reset_index()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Répartition des propositions par niveau de revenus
                    fig_revenus_pie = px.pie(
                        revenus_data,
                        names='segment_revenus',
                        values='nombre_propositions',
                        title="Répartition des propositions par niveau de revenus",
                        hole=0.4
                    )
                    fig_revenus_pie.update_traces(textinfo='percent+label')
                    st.plotly_chart(fig_revenus_pie, use_container_width=True)
                
                with col2:
                    # Taux moyens par niveau de revenus
                    fig_revenus_taux = px.bar(
                        revenus_data,
                        x='segment_revenus',
                        y='taux_moyen_hors_assurance',
                        title="Taux moyen par niveau de revenus",
                        labels={'segment_revenus': 'Niveau de revenus', 'taux_moyen_hors_assurance': 'Taux moyen (%)'},
                        color='taux_moyen_hors_assurance',
                        color_continuous_scale=px.colors.sequential.Blues,
                        text_auto='.2f'
                    )
                    fig_revenus_taux.update_traces(texttemplate='%{text}%', textposition='outside')
                    st.plotly_chart(fig_revenus_taux, use_container_width=True)
        
        with tabs_profil[2]:
            if "categorie_professionnelle" in df_taux_profil.columns:
                prof_data = df_taux_profil.groupby('categorie_professionnelle').agg({
                    'nombre_propositions': 'sum',
                    'taux_moyen_hors_assurance': 'mean'
                }).reset_index().sort_values(by='nombre_propositions', ascending=False)
                
                # Graphique des taux moyens par catégorie professionnelle
                fig_prof_taux = px.bar(
                    prof_data.head(10),
                    x='categorie_professionnelle',
                    y='taux_moyen_hors_assurance',
                    title="Taux moyen par catégorie professionnelle",
                    labels={'categorie_professionnelle': 'Catégorie professionnelle', 'taux_moyen_hors_assurance': 'Taux moyen (%)'},
                    color='taux_moyen_hors_assurance',
                    color_continuous_scale=px.colors.sequential.Blues,
                    text_auto='.2f'
                )
                fig_prof_taux.update_traces(texttemplate='%{text}%', textposition='outside')
                fig_prof_taux.update_xaxes(tickangle=45)
                st.plotly_chart(fig_prof_taux, use_container_width=True)
        
        with tabs_profil[3]:
            if "usage_bien" in df_taux_profil.columns and "type_projet" in df_taux_profil.columns:
                # Combinaison type de projet / usage du bien
                projet_data = df_taux_profil.groupby(['type_projet', 'usage_bien']).agg({
                    'nombre_propositions': 'sum',
                    'taux_moyen_hors_assurance': 'mean'
                }).reset_index()
                
                # Graphique des taux moyens par type de projet et usage du bien
                fig_projet_taux = px.bar(
                    projet_data,
                    x='type_projet',
                    y='taux_moyen_hors_assurance',
                    color='usage_bien',
                    barmode='group',
                    title="Taux moyen par type de projet et usage du bien",
                    labels={'type_projet': 'Type de projet', 'taux_moyen_hors_assurance': 'Taux moyen (%)', 'usage_bien': 'Usage du bien'},
                    text_auto='.2f'
                )
                fig_projet_taux.update_traces(texttemplate='%{text}%', textposition='outside')
                st.plotly_chart(fig_projet_taux, use_container_width=True)
    
    # Footer avec informations
    st.markdown("---")
    st.markdown("*Dashboard de performance du courtage immobilier - Données extraites de la base MotherDuck*")

if __name__ == "__main__":
    main()