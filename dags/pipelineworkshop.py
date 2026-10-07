import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import pandera as pa
from sqlalchemy import create_engine
from pathlib import Path
import sys

# CONFIGURACION DE RUTAS Y CONEXIONES
BASE_PATH = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_PATH))

# Configuración de Rutas
DATA_DIR = BASE_PATH / "data"
RAW_SPOTIFY = DATA_DIR / "raw/spotify_dataset.csv"

STAGING_SPOTIFY = DATA_DIR / "staging/spotify_staging.csv"
STAGING_GRAMMYS = DATA_DIR / "staging/grammys_staging.csv"

PROFILE_PATH = DATA_DIR / "profiling/spotify_profile.html"
QUARANTINE_PATH = DATA_DIR / "quarantine/spotify_failed.csv"
MERGED_PATH = DATA_DIR / "staging/merged_data.csv"
GOLD_CSV_PATH = DATA_DIR / "gold/final_dataset.csv"


# CONEXION A LA BASE DE DATOS desde el .env
DB_URI = os.getenv("DB_URI")


# TAREAS INDEPENDIENTES (Y SECUENCIALES para no perderme jaja)
# EXTRACCION
def read_csv_task():
    import pandas as pd
    df = pd.read_csv(RAW_SPOTIFY)
    STAGING_SPOTIFY.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(STAGING_SPOTIFY, index=False)

def read_db_task():
    engine = create_engine(DB_URI)
    df = pd.read_sql("SELECT * FROM raw_grammys;", engine)
    STAGING_GRAMMYS.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(STAGING_GRAMMYS, index=False)

# PERFILADO DE DATOS
def profile_csv_task():
    from scripts.profile_data import generate_profile
    generate_profile(str(STAGING_SPOTIFY), str(PROFILE_PATH))
