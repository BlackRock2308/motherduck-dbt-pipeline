from config.database import connect_to_motherduck, create_schema_if_not_exists, load_data_to_motherduck
from config.constants import GITHUB_OPPORTUNITIES_URL, GITHUB_PROPOSITIONS_URL, LOAD_TIMESTAMP
from config.logger import logger
import pandas as pd
import requests
import io

def download_from_github(url):
    """
    Télécharge un fichier CSV depuis GitHub et retourne un DataFrame pandas
    """
    try:
        logger.info(f"Téléchargement du fichier depuis {url}")
        response = requests.get(url)
        response.raise_for_status()

        # Convertir le contenu en objet lisible par pandas
        csv_data = io.StringIO(response.text)
        
        return pd.read_csv(csv_data, sep=',')
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement depuis {url}: {e}")
        raise

def run_etl():
    """
    Exécute le processus ETL complet
    """
    try:
        logger.info("Démarrage du processus ETL GitHub → MotherDuck")
        
        opportunities_df = download_from_github(GITHUB_OPPORTUNITIES_URL)
        propositions_df = download_from_github(GITHUB_PROPOSITIONS_URL)
        
        logger.info(f"Données téléchargées: {len(opportunities_df)} opportunités, {len(propositions_df)} propositions")
        
        conn = connect_to_motherduck()
        create_schema_if_not_exists(conn)
        
        opps_count = load_data_to_motherduck(conn, opportunities_df, "raw_opportunites", LOAD_TIMESTAMP)
        props_count = load_data_to_motherduck(conn, propositions_df, "raw_propositions", LOAD_TIMESTAMP)
                                                                                                        
        logger.info(f"Processus ETL terminé avec succès: {opps_count} opportunités et {props_count} propositions chargées")
        
    except Exception as e:
        logger.error(f"Erreur dans le processus ETL: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Connexion à MotherDuck fermée")
