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

def validate_csv_task():
    df = pd.read_csv(STAGING_SPOTIFY)
    schema = pa.DataFrameSchema({
        "track_name": pa.Column(pa.String, nullable=True),
        "artists": pa.Column(pa.String, nullable=False),
        "popularity": pa.Column(pa.Int, pa.Check.in_range(0, 100), nullable=True),
    })
    
    try:
        schema.validate(df)
    except pa.errors.SchemaError as exc:
        QUARANTINE_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(QUARANTINE_PATH, index=False)
        raise ValueError(f"Fallo de calidad de datos: {exc}")


def quarantine_data_task():
    print(f"Lote en cuarentena movido a {QUARANTINE_PATH}")


# TRANSFORMACION Y MERGE
def transform_and_merge_task():
    df_spotify = pd.read_csv(STAGING_SPOTIFY)
    df_grammys = pd.read_csv(STAGING_GRAMMYS)
    
    # Transformación: Limpiar artista para poder cruzar (Spotify)
    df_spotify['artist_clean'] = df_spotify['artists'].astype(str).str.split(';').str[0].str.lower().str.strip()
    
    # Transformación: Limpiar artista (Grammys)
    df_grammys['artist_clean'] = df_grammys['worker'].astype(str).str.lower().str.strip()
    
    # Merge (Left Join para no perder las canciones de Spotify)
    df_merged = pd.merge(df_spotify, df_grammys, on='artist_clean', how='left')
    df_merged.to_csv(MERGED_PATH, index=False)

# CARGA A LA BASE DE DATOS
def load_db_task():
    df = pd.read_csv(MERGED_PATH)
    engine = create_engine(DB_URI)
    df.to_sql('gold_spotify_grammys', engine, if_exists='replace', index=False)

def store_csv_task():
    df = pd.read_csv(MERGED_PATH)
    GOLD_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(GOLD_CSV_PATH, index=False)

default_args = {
    "owner": "urbano",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="workshop2_pipeline",
    default_args=default_args,
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    op_read_csv = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv_task)
    
    op_read_db = PythonOperator(
        task_id="read_db",
        python_callable=read_db_task)
    
    op_profile = PythonOperator(
        task_id="profile_csv",
        python_callable=profile_csv_task)
    
    op_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_csv_task)
    op_quarantine = PythonOperator(
        task_id="quarantine",
        python_callable=quarantine_data_task,
        trigger_rule="one_failed")
    
    op_merge = PythonOperator(
        task_id="merge_datasets",
        python_callable=transform_and_merge_task,
        trigger_rule="all_success")
    
    op_load = PythonOperator(
        task_id="load_to_db",
        python_callable=load_db_task)
    
    op_store = PythonOperator(
        task_id="store_to_csv",
        python_callable=store_csv_task)

    # Flujo Spotify
    op_read_csv >> op_profile >> op_validate
    op_validate >> [op_merge, op_quarantine]
    
    # Flujo Grammys (Paralelo)
    op_read_db >> op_merge
    
    # Carga y guardado final
    op_merge >> [op_load, op_store]

