from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path

# CONFIGURACION DE RUTAS Y CONEXIONES
BASE_PATH = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_PATH / "data"

RAW_CSV_PATH = DATA_DIR / "raw/spotify_dataset.csv"
STAGING_CSV_PATH = DATA_DIR / "staging/spotify_staging.csv"
STAGING_DB_PATH = DATA_DIR / "staging/grammys_staging.csv"
PROFILE_PATH = DATA_DIR / "profiling/spotify_profile.html"
TRANSFORMED_CSV_PATH = DATA_DIR / "staging/spotify_transformed.csv"
TRANSFORMED_DB_PATH = DATA_DIR / "staging/grammys_transformed.csv"
MERGED_PATH = DATA_DIR / "staging/merged_data.csv"
GOLD_PATH = DATA_DIR / "gold/final_dataset.csv"
QUARANTINE_PATH = DATA_DIR / "quarantine/spotify_failed.csv"


# CONEXION A BD workshop_db IMPORTANTE CAMBIAR USUARIO Y CONTRASENA
DB_URI = "postgresql://usuario:password@localhost:5432/workshop_db"


# TAREAS INDEPENDIENTES (Y SECUENCIALES para no perderme jaja)
def read_csv_task():
    import pandas as pd
    df = pd.read_csv(RAW_CSV_PATH)
    df.to_csv(STAGING_CSV_PATH, index=False)

def read_db_task():
    import pandas as pd
    from sqlalchemy import create_engine
    engine = create_engine(DB_URI)
    
