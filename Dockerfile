FROM prefecthq/prefect:3-latest

# Installation des dépendances système
USER root
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copier les requirements
COPY requirements.txt /tmp/requirements.txt

# Installer les dépendances Python
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copier le code source
COPY src/ /app/src/
COPY deployments/ /app/deployments/
COPY data/ /app/data/

# Définir le répertoire de travail
WORKDIR /app

# Commande par défaut
CMD ["prefect", "worker", "start", "--pool", "local-pool"]