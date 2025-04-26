import duckdb
from config.logger import logger
from config.constants import DATABASE_NAME


def connect_to_motherduck():
    """
    Établit une connexion à MotherDuck et attache la base de données
    """
    try:
        logger.info("Connexion à MotherDuck")

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
    Loads DataFrame to MotherDuck after converting to Parquet
    """
    try:
        logger.info(f"Loading data into source.{table_name}")
        
        # Add tracking columns
        df['_loaded_at'] = load_timestamp
        df['_source_file'] = table_name
        
        # Write to temporary parquet file
        temp_parquet_path = f"/tmp/{table_name}.parquet"
        df.to_parquet(temp_parquet_path, index=False)
        
        # Create or replace table using Parquet
        conn.execute(f"CREATE OR REPLACE TABLE source.{table_name} AS SELECT * FROM read_parquet('{temp_parquet_path}')")
        
        record_count = conn.execute(f"SELECT COUNT(*) FROM source.{table_name}").fetchone()[0]
        logger.info(f"{record_count} records loaded into source.{table_name}")
        
        return record_count
    except Exception as e:
        logger.error(f"Error loading source.{table_name}: {e}")
        raise