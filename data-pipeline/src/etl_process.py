from extract import download_from_github
from database import connect_to_motherduck, create_schema_if_not_exists, load_data_to_motherduck
from logger import logger
from config import GITHUB_OPPORTUNITIES_URL, GITHUB_PROPOSITIONS_URL, LOAD_TIMESTAMP


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
