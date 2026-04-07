from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session


def build_fact_actividad(session: Session) -> None:
    from snowflake.snowpark import functions as F

    raw = session.table("RAW.ACTIVIDAD_ESTUDIANTES")
    dim_est = session.table("MART.DIM_ESTUDIANTE")
    dim_tiempo = session.table("MART.DIM_TIEMPO")
    dim_geo = session.table("MART.DIM_GEOGRAFICA")
    dim_ctx = session.table("MART.DIM_CONTEXTO")

    staged = raw.select(
        "ID_PERSONA",
        "SEXO",
        "DEPARTAMENTO",
        "ZONA",
        "SUBSISTEMA",
        "CICLO",
        "GRADO",
        "CONTEXTO",
        F.col("ANIO_LECTIVO").cast("INTEGER").alias("ANIO_LECTIVO"),
        F.col("CREA_DIAS_INGRESO").cast("FLOAT").alias("CREA_DIAS_INGRESO"),
        F.col("CREA_ENTREGAS_TAREAS").cast("FLOAT").alias("CREA_ENTREGAS_TAREAS"),
        F.col("CREA_COMENTARIOS").cast("FLOAT").alias("CREA_COMENTARIOS"),
        F.col("CREA_ACCIONES_TOTALES").cast("FLOAT").alias("CREA_ACCIONES_TOTALES"),
        F.col("MATIFIC_DIAS_INGRESO").cast("FLOAT").alias("MATIFIC_DIAS_INGRESO"),
        F.col("MATIFIC_EPISODIOS").cast("FLOAT").alias("MATIFIC_EPISODIOS"),
        F.col("BIBLIOTECA_DIAS_INGRESO").cast("FLOAT").alias("BIBLIOTECA_DIAS_INGRESO"),
        F.col("BIBLIOTECA_PRESTAMOS").cast("FLOAT").alias("BIBLIOTECA_PRESTAMOS"),
    )

    fact = (
        staged
        .join(
            dim_est.select("SK_ESTUDIANTE", "ID_PERSONA"),
            on="ID_PERSONA",
            how="left",
        )
        .join(
            dim_tiempo.select("SK_TIEMPO", "ANIO_LECTIVO"),
            on="ANIO_LECTIVO",
            how="left",
        )
        .join(
            dim_geo.select("SK_GEOGRAFICA", "DEPARTAMENTO", "ZONA"),
            on=["DEPARTAMENTO", "ZONA"],
            how="left",
        )
        .join(
            dim_ctx.select("SK_CONTEXTO", "SUBSISTEMA", "CICLO", "GRADO", "CONTEXTO"),
            on=["SUBSISTEMA", "CICLO", "GRADO", "CONTEXTO"],
            how="left",
        )
        .select(
            "SK_ESTUDIANTE",
            "SK_TIEMPO",
            "SK_GEOGRAFICA",
            "SK_CONTEXTO",
            "CREA_DIAS_INGRESO",
            "CREA_ENTREGAS_TAREAS",
            "CREA_COMENTARIOS",
            "CREA_ACCIONES_TOTALES",
            "MATIFIC_DIAS_INGRESO",
            "MATIFIC_EPISODIOS",
            "BIBLIOTECA_DIAS_INGRESO",
            "BIBLIOTECA_PRESTAMOS",
        )
    )

    count = fact.count()
    fact.write.save_as_table("MART.FACT_ACTIVIDAD", mode="overwrite")
    print(f"FACT_ACTIVIDAD: {count:,} filas")
