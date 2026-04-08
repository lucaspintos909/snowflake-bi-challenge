# snowflake-bi-challenge

ELT pipeline — Evaluación técnica BI Analista desarrollador, Ceibal (Uruguay).

Ingesta el dataset público de actividad de estudiantes en plataformas educativas (CREA, Matific, Biblioteca País) y lo transforma en un star schema en Snowflake usando Python y Snowpark.

Datos públicos: [catalogodatos.gub.uy](https://catalogodatos.gub.uy/dataset/centro_ceibal-actividad-en-plataformas-educativas-estudiantes)

---

## Arquitectura de la solución

![Arquitectura de la solución](docs/images/Diagrama_1.png)

---

## Modelo de datos — Modelo estrella (Kimball)

![Modelo estrella](docs/images/Modelo_estrella.png)


**Granularidad de FACT_ACTIVIDAD:** una fila por estudiante x año lectivo x contexto educativo x geografía.

**Métricas disponibles:** días de ingreso, entregas, comentarios y acciones en CREA; días y episodios en Matific; días y préstamos en Biblioteca País.

**Por qué esquema estrella?**
Permite filtrar por cualquier dimensión y agregar métricas en una query simple con JOINs predecibles. Snowflake optimiza bien los JOINs entre una fact table grande y dimensiones pequeñas.

**Por qué una capa RAW?**
Para dejar el dato original intacto (todo VARCHAR, copia fiel del CSV). Las transformaciones de tipos y limpieza ocurren en Snowpark al construir MART, sin destruir la fuente.

**Por qué no hay una capa STAGING explícita?**
La limpieza y tipado de datos ocurre inline en Snowpark al construir cada tabla de MART. Cada función de transformación hace el cast de tipos y normalización directamente sobre RAW, cumpliendo el mismo rol que una capa STAGING sin materializarla como tabla intermedia. Dado que el pipeline se ejecuta end-to-end desde RAW en cada corrida, una tabla intermedia no aportaría trazabilidad adicional ni reutilización entre pasos.

---

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Completar credenciales de Snowflake en .env
```

Variables requeridas en `.env`:

```
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_ROLE=
```

### Configuración del pipeline

Los nombres de tablas, schemas, stage y ruta del CSV se configuran en `pipeline_config.yaml`:

```yaml
csv:
  path: actividad-de-estudiantes-2025.csv  # ruta al CSV (relativa al directorio raíz)

snowflake:
  database: CEIBAL_DB
  raw_schema: RAW
  mart_schema: MART
  stage: RAW_STAGE
  raw_table: ACTIVIDAD_ESTUDIANTES
```

Modificar este archivo para apuntar a otro CSV o usar nombres de objetos distintos en Snowflake. Las credenciales siguen en `.env` y nunca se commitean.

---

## Correr el pipeline

Descargar el CSV y ubicarlo en la raíz del proyecto (o actualizar `csv.path` en `config/pipeline.yaml`):
[actividad-de-estudiantes-2025.csv](https://catalogodatos.gub.uy/dataset/caf9f327-4446-4326-a6c2-450cfacf8446/resource/eb7ab748-4b66-41ab-9145-6ce0ef8ba2cb/download/actividad-de-estudiantes-2025.csv)

```bash
python main.py                  # pipeline completo (ingesta + transformaciones)
python main.py --ingest-only    # solo carga el CSV a RAW
python main.py --transform-only # solo construye el modelo en MART
```
