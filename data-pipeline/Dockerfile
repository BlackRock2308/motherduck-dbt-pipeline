FROM python:3.10-slim

# Métadonnées
LABEL maintainer="votre@email.com"
LABEL description="ETL GitHub vers MotherDuck pour données de courtage immobilier"

# Installation des dépendances système
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Définition du répertoire de travail
WORKDIR /app

# Copie des fichiers de dépendances et installation
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY etl_github_to_motherduck.py .

# Définition des variables d'environnement par défaut
ENV GITHUB_OPPORTUNITIES_URL="https://github.com/[votre-username]/[repo-name]/raw/main/opportunites.csv"
ENV GITHUB_PROPOSITIONS_URL="https://github.com/[votre-username]/[repo-name]/raw/main/proposition.csv"
ENV DATABASE_NAME="immobilier_courtage"

# Commande par défaut
CMD ["python", "etl_github_to_motherduck.py"]