#!/bin/bash
# Shebang line, indicating the script should be run with Bash

set -e
# Exit immediately if a command exits with a non-zero status

if [ -e "/opt/airflow/requirements.txt" ]; then
    # Check if the file /opt/airflow/requirements.txt exists
    $(command python) pip install --upgrade pip
    # Upgrade pip using the Python interpreter found in the system's PATH
    $(command -v pip) install --user -r requirements.txt
    # Install Python packages listed in requirements.txt for the user
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
    # Check if the file /opt/airflow/airflow.db does NOT exist
    airflow db init &&
    # Initialize the Airflow database
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password admin
    # Create an Airflow admin user
fi

$(command -v airflow) db upgrade
# Upgrade the Airflow database to the latest version

exec airflow webserver
# Start the Airflow webserver