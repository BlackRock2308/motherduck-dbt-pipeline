import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

DATABASE_NAME = os.getenv('DATABASE_NAME', 'immobilier_courtage')

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Courtage Immobilier",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

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

# Connexion à MotherDuck
@st.cache_resource
def get_motherduck_connection():
    """Établit une connexion à MotherDuck et la met en cache"""
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        st.error("Le token MotherDuck est manquant. Veuillez le définir dans les variables d'environnement.")
        st.stop()
    conn_string = f"md:immobilier_courtage?MOTHERDUCK_TOKEN={token}"
    try:
        return duckdb.connect(conn_string)
    except Exception as e:
        st.error(f"Erreur de connexion à MotherDuck: {e}")
        st.stop()

# Fonction générique pour charger les données
@st.cache_data(ttl=3600)
def load_data(query, periode=None, date_column=None):
    """Charge les données depuis la base avec un filtre de période facultatif"""
    conn = get_connect_to_motherduck()
    #conn = get_motherduck_connection()
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
    return load_data("SELECT * FROM main_gold.taux_conversion_opportunites", periode, "mois_creation")

def load_taux_profil():
    return load_data("SELECT * FROM main_gold.taux_par_profil")

def load_performance_source(periode):
    return load_data("SELECT * FROM main_gold.performance_source", periode, "mois_acquisition")

# Interface utilisateur Streamlit
def main():
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
    taux_conversion_global = (total_converties / total_opportunites * 100) if total_opportunites > 0 else 0
    
    #col1.metric("Nombre d'opportunités", f"{total_opportunites:,}".replace(",", " "))
    col1.metric("🏦 Nombre de banques", df_banques['partenaire_id'].nunique())
    col2.metric("💰 Nombre de propositions", f"{df_banques['nombre_propositions'].sum():,}".replace(",", " ") if not df_banques.empty else 0)
    #col3.metric("☁️ Taux de conversion", f"{taux_conversion_global:.1f}%")
    col3.metric("☁️ Montant moyen du prêt", f"{df_banques['montant_moyen'].mean():,.0f} €")
    col4.metric("🌟 Taux moyen hors assurance", f"{df_banques['taux_moyen_hors_assurance'].mean():.2f}%" if not df_banques.empty else "0.00%")
    
    # Graphiques - Performance par source
    st.header("Performance par source d'acquisition")
    
    tabs = st.tabs(["Vue générale", "Conversion", "Montants", "Évolution temporelle"])
    
    with tabs[0]:
        if not df_perf_source.empty:
            # Création d'un tableau de performance par source
            perf_agg = df_perf_source.groupby('origine').agg({
                'nombre_opportunites': 'sum',
                'nombre_converties': 'sum',
                'taux_conversion': 'mean',
                'gain_realise': 'sum',
                'montant_moyen_pret': 'mean'
            }).reset_index()
            
            perf_agg['taux_conversion'] = perf_agg['nombre_converties'] / perf_agg['nombre_opportunites'] * 100
            
            # Affichage du tableau
            st.dataframe(
                perf_agg.rename(columns={
                    'origine': 'Source',
                    'nombre_opportunites': 'Opportunités',
                    'nombre_converties': 'Conversions',
                    'taux_conversion': 'Taux de conversion (%)',
                    'gain_realise': 'Gain réalisé (€)',
                    'montant_moyen_pret': 'Montant moyen (€)'
                }).style.format({
                    'Taux de conversion (%)': '{:.1f}%',
                    'Gain réalisé (€)': '{:,.0f} €',
                    'Montant moyen (€)': '{:,.0f} €'
                }),
                use_container_width=True
            )
    
    with tabs[1]:
        # Graphique de conversion par source
        if not df_perf_source.empty:
            # Agrégation par source pour le graphique
            conversion_by_source = df_perf_source.groupby('origine').agg({
                'nombre_opportunites': 'sum',
                'nombre_converties': 'sum'
            }).reset_index()
            
            conversion_by_source['taux_conversion'] = conversion_by_source['nombre_converties'] / conversion_by_source['nombre_opportunites'] * 100
            
            fig_conversion = px.bar(
                conversion_by_source.sort_values(by='taux_conversion', ascending=False),
                x='origine',
                y='taux_conversion',
                text_auto='.1f',
                title="Taux de conversion par source d'acquisition (%)",
                labels={'origine': 'Source', 'taux_conversion': 'Taux de conversion (%)'},
                color='taux_conversion',
                color_continuous_scale=px.colors.sequential.Blues
            )
            fig_conversion.update_traces(texttemplate='%{text}%', textposition='outside')
            st.plotly_chart(fig_conversion, use_container_width=True)
    
    with tabs[2]:
        # Graphique des montants moyens par source
        if not df_perf_source.empty:
            fig_montant = px.bar(
                df_perf_source.groupby('origine').agg({'montant_moyen_pret': 'mean'}).reset_index().sort_values(by='montant_moyen_pret', ascending=False),
                x='origine',
                y='montant_moyen_pret',
                title="Montant moyen des prêts par source d'acquisition",
                labels={'origine': 'Source', 'montant_moyen_pret': 'Montant moyen (€)'},
                color='montant_moyen_pret',
                color_continuous_scale=px.colors.sequential.Greens,
                text_auto='.0f'
            )
            fig_montant.update_traces(texttemplate='%{text:,.0f} €', textposition='outside')
            st.plotly_chart(fig_montant, use_container_width=True)
    
    with tabs[3]:
        # Graphique d'évolution temporelle
        if not df_perf_source.empty:
            # Conversion de la date en format datetime
            df_perf_source['mois_acquisition'] = pd.to_datetime(df_perf_source['mois_acquisition'])
            df_perf_source = df_perf_source.sort_values('mois_acquisition')
            
            fig_evolution = px.line(
                df_perf_source,
                x="mois_acquisition",
                y="nombre_opportunites",
                color="origine",
                title="Évolution du nombre d'opportunités par source",
                labels={"mois_acquisition": "Mois", "nombre_opportunites": "Nombre d'opportunités", "origine": "Source"}
            )
            
            # Ajout de marqueurs pour une meilleure lisibilité
            fig_evolution.update_traces(mode='lines+markers')
            st.plotly_chart(fig_evolution, use_container_width=True)
            
            # Évolution du taux de conversion dans le temps
            conversion_time = df_perf_source.groupby(['mois_acquisition', 'origine']).agg({
                'nombre_opportunites': 'sum',
                'nombre_converties': 'sum'
            }).reset_index()
            
            conversion_time['taux_conversion'] = conversion_time['nombre_converties'] / conversion_time['nombre_opportunites'] * 100
            
            fig_conv_time = px.line(
                conversion_time,
                x="mois_acquisition",
                y="taux_conversion",
                color="origine",
                title="Évolution du taux de conversion par source",
                labels={"mois_acquisition": "Mois", "taux_conversion": "Taux de conversion (%)", "origine": "Source"}
            )
            fig_conv_time.update_traces(mode='lines+markers')
            st.plotly_chart(fig_conv_time, use_container_width=True)
    
    # Analyse des données bancaires
    st.header("Performance des partenaires bancaires")
    
    if not df_banques.empty:
        # Tri des banques par nombre de propositions
        top_banques = df_banques.sort_values(by='nombre_propositions', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Graphique des propositions par banque
            fig_banques = px.bar(
                top_banques.head(10),
                x='partenaire_id',
                y='nombre_propositions',
                title="Top 10 des banques par nombre de propositions",
                labels={'partenaire_id': 'Banque', 'nombre_propositions': 'Nombre de propositions'},
                color='nombre_propositions',
                color_continuous_scale=px.colors.sequential.Blues,
                text_auto=True
            )
            fig_banques.update_traces(textposition='outside')
            st.plotly_chart(fig_banques, use_container_width=True)
        
        with col2:
            # Taux moyens par banque
            fig_taux = px.bar(
                top_banques.head(10),
                x='partenaire_id',
                y='taux_moyen_hors_assurance',
                title="Taux moyens hors assurance par banque partenaire",
                labels={'partenaire_id': 'Banque', 'taux_moyen_hors_assurance': 'Taux moyen (%)'},
                color='taux_moyen_hors_assurance',
                color_continuous_scale=px.colors.sequential.Reds_r,  # Inversé: plus le taux est bas, mieux c'est
                text_auto='.2f'
            )
            fig_taux.update_traces(texttemplate='%{text}%', textposition='outside')
            st.plotly_chart(fig_taux, use_container_width=True)
        
        # Analyse par segment
        st.subheader("Taux moyens par segment de marché")
        
        segment_cols = [
            ('taux_moyen_residence_principale', 'Résidence principale'),
            ('taux_moyen_investissement_locatif', 'Investissement locatif'),
            ('taux_moyen_salarie_prive', 'Salarié privé'),
            ('taux_moyen_fonctionnaire', 'Fonctionnaire')
        ]
        
        segment_data = {name: [] for _, name in segment_cols}
        segment_data['Banque'] = []
        
        for _, row in top_banques.head(10).iterrows():
            segment_data['Banque'].append(row['partenaire_id'])
            for col, name in segment_cols:
                if col in row and not pd.isna(row[col]):
                    segment_data[name].append(row[col])
                else:
                    segment_data[name].append(None)
        
        segment_df = pd.DataFrame(segment_data)
        segment_long = pd.melt(segment_df, id_vars=['Banque'], var_name='Segment', value_name='Taux')
        
        if not segment_long.empty:
            fig_segments = px.bar(
                segment_long.dropna(),
                x='Banque',
                y='Taux',
                color='Segment',
                barmode='group',
                title="Taux moyens par segment et par banque",
                labels={'Banque': 'Banque', 'Taux': 'Taux moyen (%)', 'Segment': 'Segment de marché'}
            )
            st.plotly_chart(fig_segments, use_container_width=True)

        
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