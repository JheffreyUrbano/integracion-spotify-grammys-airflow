# scripts/init_db.py
import os
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=True, encoding="utf-8")


# Conexión a Docker PostgreSQL
DB_URI = os.getenv("DB_URI")

# Rutas
BASE_PATH = Path(__file__).resolve().parents[1]
GRAMMYS_CSV = BASE_PATH / "data/raw/the_grammy_awards.csv"

def init_database():
    print("Leyendo the_grammy_awards.csv...")
    df = pd.read_csv(GRAMMYS_CSV)
    
    print("Conectando a PostgreSQL...")
    engine = create_engine(DB_URI)
    
    print("Cargando datos a la tabla 'raw_grammys'...")
    df.to_sql('raw_grammys', engine, if_exists='replace', index=False)
    print("¡Base de datos inicializada con éxito!")

if __name__ == "__main__":
    init_database()