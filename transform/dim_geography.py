from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session


def build_dim_geografica(session: Session) -> None:
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    raw = session.table("RAW.ACTIVIDAD_ESTUDIANTES")
    dim = (
        raw
        .select("DEPARTAMENTO", "ZONA")
        .with_column("ZONA", F.when(F.col("ZONA") == "Sin Datos", F.lit("Sin Dato")).otherwise(F.col("ZONA")))
        .with_column("DEPARTAMENTO", F.when(F.col("DEPARTAMENTO") == "Sin Datos", F.lit("Sin Dato")).otherwise(F.col("DEPARTAMENTO")))
        .distinct()
        .sort("DEPARTAMENTO", "ZONA")
    )
    window = Window.order_by(F.col("DEPARTAMENTO"), F.col("ZONA"))
    dim = dim.with_column("SK_GEOGRAFICA", F.row_number().over(window))
    dim = dim.select("SK_GEOGRAFICA", "DEPARTAMENTO", "ZONA")
    count = dim.count()
    dim.write.save_as_table("MART.DIM_GEOGRAFICA", mode="overwrite")
    print(f"DIM_GEOGRAFICA: {count} filas")
