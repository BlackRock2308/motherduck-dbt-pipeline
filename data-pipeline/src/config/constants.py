import os
from datetime import datetime

GITHUB_OPPORTUNITIES_URL = os.getenv(
    'GITHUB_OPPORTUNITIES_URL',
    'https://raw.githubusercontent.com/BlackRock2308/response-test-technique-meilleurtaux/refs/heads/main/data_sources/data_samples/opportunity_test.csv'
)
GITHUB_PROPOSITIONS_URL = os.getenv(
    'GITHUB_PROPOSITIONS_URL',
    'https://raw.githubusercontent.com/BlackRock2308/response-test-technique-meilleurtaux/refs/heads/main/data_sources/data_samples/propositions_test.csv'
)
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN', '')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'immobilier_courtage')
LOAD_TIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')



