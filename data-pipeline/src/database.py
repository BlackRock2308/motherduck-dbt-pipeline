import os
import duckdb
from logger import logger
from config import DATABASE_NAME


def connect_to_motherduck():
    """
    Établit une connexion à MotherDuck et attache la base de données
    """
    try:
        logger.info("Connexion à MotherDuck")

        # Connexion à DuckDB en mode MotherDuck
        conn = duckdb.connect(f"md:{DATABASE_NAME}", read_only=False)

        logger.info(f"Base de données active: {DATABASE_NAME}")

        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion à MotherDuck: {e}")
        raise


def create_schema_if_not_exists(conn):
    """
    Crée le schéma source s'il n'existe pas déjà
    """
    try:
        logger.info("Création du schéma source si nécessaire")
        conn.execute("CREATE SCHEMA IF NOT EXISTS source")
    except Exception as e:
        logger.error(f"Erreur lors de la création du schéma: {e}")
        raise


def load_data_to_motherduck(conn, df, table_name, load_timestamp):
    """
    Charge un DataFrame dans une table MotherDuck avec des colonnes de traçabilité
    """
    try:
        logger.info(f"Chargement des données dans source.{table_name}")
        
        df['_loaded_at'] = load_timestamp
        df['_source_file'] = table_name
        
        conn.execute(f"CREATE OR REPLACE TABLE source.{table_name} AS SELECT * FROM df")
        
        record_count = conn.execute(f"SELECT COUNT(*) FROM source.{table_name}").fetchone()[0]
        logger.info(f"{record_count} enregistrements chargés dans source.{table_name}")
        
        return record_count
    except Exception as e:
        logger.error(f"Erreur lors du chargement de source.{table_name}: {e}")
        raise
