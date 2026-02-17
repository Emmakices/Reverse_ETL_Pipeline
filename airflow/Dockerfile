FROM apache/airflow:2.9.3

# ──────────────────────────────────────────────────────────────
# 1. Install system dependencies as root
#    - ODBC Driver 18 for SQL Server (required by pyodbc / azure_sql.py)
#    - curl + gnupg for Microsoft package repo setup
# ──────────────────────────────────────────────────────────────
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        unixodbc-dev \
        g++ && \
    # Add Microsoft package signing key and repo
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
    # Cleanup
    apt-get purge -y --auto-remove gnupg && \
    rm -rf /var/lib/apt/lists/*

# ──────────────────────────────────────────────────────────────
# 2. Install Python dependencies as airflow user
# ──────────────────────────────────────────────────────────────
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
