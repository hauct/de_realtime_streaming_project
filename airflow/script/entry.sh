#!/bin/bash
# Shebang line, indicating the script should be run with Bash

set -e
# Exit immediately if a command exits with a non-zero status

if [ -e "/opt/airflow/requirements.txt" ]; then
    # Check if the file /opt/airflow/requirements.txt exists
    $(command python) pip install --upgrade pip
    # Upgrade pip using the Python interpreter found in the system's PATH
    $(command -v pip) install --user -r /opt/airflow/requirements.txt
    # Install Python packages listed in requirements.txt for the user
fi

exec airflow standalone