from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session
    from utils.config import PipelineConfig


def build_dim_estudiante(session: Session, config: PipelineConfig) -> None:
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    raw = session.table(f"{config.raw_schema}.{config.raw_table}")
    candidates = (
        raw
        .select("ID_PERSONA", "SEXO")
        .distinct()
    )

    dim_fqn = f"{config.mart_schema}.DIM_ESTUDIANTE"

    try:
        existing = session.table(dim_fqn)
        existing.limit(0).collect()
        max_sk = existing.select(F.max(F.col("SK_ESTUDIANTE"))).collect()[0][0] or 0
        nuevas = candidates.join(
            existing.select("ID_PERSONA"), on="ID_PERSONA", how="left_anti"
        )
        if nuevas.count() == 0:
            print("DIM_ESTUDIANTE: sin cambios")
            return
        window = Window.order_by(F.col("ID_PERSONA"))
        nuevas = nuevas.with_column("SK_ESTUDIANTE", F.lit(max_sk) + F.row_number().over(window))
        nuevas = nuevas.select("SK_ESTUDIANTE", "ID_PERSONA", "SEXO")
        count = nuevas.count()
        nuevas.write.save_as_table(dim_fqn, mode="append")
        print(f"DIM_ESTUDIANTE: +{count} filas nuevas")
    except Exception:
        window = Window.order_by(F.col("ID_PERSONA"))
        dim = candidates.with_column("SK_ESTUDIANTE", F.row_number().over(window))
        dim = dim.select("SK_ESTUDIANTE", "ID_PERSONA", "SEXO")
        count = dim.count()
        dim.write.save_as_table(dim_fqn, mode="overwrite")
        print(f"DIM_ESTUDIANTE: {count} filas")
