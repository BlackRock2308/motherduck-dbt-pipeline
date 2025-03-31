import pandas as pd
import requests
import io
from logger import logger


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
