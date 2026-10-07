#!/usr/bin/env bash
set -e

echo "Iniciando contenedor de PostreSQL..."
docker-compose up -d

sleep 5

python scripts/init_db.py

# Configuración de Airflow
export AIRFLOW_HOME="$(pwd)/.airflow"

# ruta de los dags
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"

export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX="True"
export PYTHONPATH="$(pwd)"

echo "Inicializando Airflow..."
airflow db migrate

echo "Iniciando Airflow standalone..."
exec airflow standalone