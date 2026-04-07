from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session


def build_dim_geografia(session: Session) -> None:
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    raw = session.table("RAW.ACTIVIDAD_ESTUDIANTES")
    dim = (
        raw
        .select("DEPARTAMENTO", "ZONA")
        .distinct()
        .sort("DEPARTAMENTO", "ZONA")
    )
    window = Window.order_by(F.col("DEPARTAMENTO"), F.col("ZONA"))
    dim = dim.with_column("SK_GEOGRAFIA", F.row_number().over(window))
    dim = dim.select("SK_GEOGRAFIA", "DEPARTAMENTO", "ZONA")
    count = dim.count()
    dim.write.save_as_table("MART.DIM_GEOGRAFIA", mode="overwrite")
    print(f"DIM_GEOGRAFIA: {count} filas")
