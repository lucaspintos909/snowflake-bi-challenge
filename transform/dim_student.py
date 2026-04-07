from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session


def build_dim_estudiante(session: Session) -> None:
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    raw = session.table("RAW.ACTIVIDAD_ESTUDIANTES")
    dim = (
        raw
        .select("ID_PERSONA", "SEXO")
        .distinct()
        .sort("ID_PERSONA")
    )
    window = Window.order_by(F.col("ID_PERSONA"))
    dim = dim.with_column("SK_ESTUDIANTE", F.row_number().over(window))
    dim = dim.select("SK_ESTUDIANTE", "ID_PERSONA", "SEXO")
    count = dim.count()
    dim.write.save_as_table("MART.DIM_ESTUDIANTE", mode="overwrite")
    print(f"DIM_ESTUDIANTE: {count} filas")
