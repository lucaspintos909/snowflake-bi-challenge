# snowflake-bi-challenge

ELT pipeline — Evaluación técnica BI Analista desarrollador, Ceibal (Uruguay).

Ingesta el dataset público de actividad de estudiantes en plataformas educativas (CREA, Matific, Biblioteca País) y lo transforma en un star schema en Snowflake usando Python y Snowpark.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Completar credenciales de Snowflake en .env
```

## Correr el pipeline

```bash
python main.py                    # pipeline completo
python main.py --ingest-only      # solo ingesta
python main.py --transform-only   # solo transformaciones
```

