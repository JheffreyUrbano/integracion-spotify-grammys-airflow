# integracion-spotify-grammys-airflow
Pipeline ETL automatizado con Apache Airflow para la extracción, validación y consolidación de datos desde múltiples fuentes (CSV y Base de Datos).

## 1. Descripción del Proyecto
Este repositorio contiene la implementación de un pipeline ETL automatizado utilizando Apache Airflow. El objetivo principal es extraer información de dos fuentes de datos distintas (un archivo CSV de Spotify y una base de datos de Grammys), realizar perfilado, aplicar validaciones de calidad de datos, transformar y cruzar la información, para finalmente cargar los resultados en una base de datos PostgreSQL y exportarlos como un archivo CSV local.

## 2. Estructura del Repositorio
La arquitectura del proyecto sigue las mejores prácticas de separación por capas de datos (RAW, STAGING, PROFILING, GOLD, QUARANTINE).

```text
workshop_02/
├── dags/
│   └── pipeline_workshop.py
├── data/
│   ├── raw/                 # Archivos originales de Kaggle
│   ├── staging/             # Archivos intermedios del procesamiento
│   ├── profiling/           # Reportes generados por ydata-profiling
│   ├── gold/                # Dataset final fusionado
│   └── quarantine/          # Registros que no superan el contrato de datos
├── scripts/
│   ├── init_db.py           # Script para inicializar PostgreSQL
│   └── profile_data.py      # Lógica de generación del reporte de calidad
├── docker-compose.yml       # Configuración del contenedor de PostgreSQL
└── README.md