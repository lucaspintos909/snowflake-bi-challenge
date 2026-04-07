# snowflake-bi-challenge

ELT pipeline — Evaluación técnica BI Analista desarrollador, Ceibal (Uruguay).

Ingesta el dataset público de actividad de estudiantes en plataformas educativas (CREA, Matific, Biblioteca País) y lo transforma en un star schema en Snowflake usando Python y Snowpark.

Datos públicos obtenidos de: https://catalogodatos.gub.uy/dataset/centro_ceibal-actividad-en-plataformas-educativas-estudiantes

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Completar credenciales de Snowflake en .env
```

## Correr el pipeline

Descargar el csv de datos en: https://catalogodatos.gub.uy/dataset/caf9f327-4446-4326-a6c2-450cfacf8446/resource/eb7ab748-4b66-41ab-9145-6ce0ef8ba2cb/download/actividad-de-estudiantes-2025.csv

Ubicarlo en la raiz del proyecto.

Ejecutar el pipeline:
```bash
python main.py                    # pipeline completo
python main.py --ingest-only      # solo ingesta
python main.py --transform-only   # solo transformaciones
```

