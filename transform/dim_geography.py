from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session
    from utils.config import PipelineConfig


def build_dim_geografia(session: Session, config: PipelineConfig) -> None:
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    raw = session.table(f"{config.raw_schema}.{config.raw_table}")
    candidates = (
        raw
        .select("DEPARTAMENTO", "ZONA")
        .distinct()
    )

    dim_fqn = f"{config.mart_schema}.DIM_GEOGRAFIA"

    try:
        existing = session.table(dim_fqn)
        existing.limit(0).collect()
        max_sk = existing.select(F.max(F.col("SK_GEOGRAFIA"))).collect()[0][0] or 0
        nuevas = candidates.join(
            existing.select("DEPARTAMENTO", "ZONA"),
            on=["DEPARTAMENTO", "ZONA"],
            how="left_anti",
        )
        if nuevas.count() == 0:
            print("DIM_GEOGRAFIA: sin cambios")
            return
        window = Window.order_by(F.col("DEPARTAMENTO"), F.col("ZONA"))
        nuevas = nuevas.with_column("SK_GEOGRAFIA", F.lit(max_sk) + F.row_number().over(window))
        nuevas = nuevas.select("SK_GEOGRAFIA", "DEPARTAMENTO", "ZONA")
        count = nuevas.count()
        nuevas.write.save_as_table(dim_fqn, mode="append")
        print(f"DIM_GEOGRAFIA: +{count} filas nuevas")
    except Exception:
        window = Window.order_by(F.col("DEPARTAMENTO"), F.col("ZONA"))
        dim = candidates.with_column("SK_GEOGRAFIA", F.row_number().over(window))
        dim = dim.select("SK_GEOGRAFIA", "DEPARTAMENTO", "ZONA")
        count = dim.count()
        dim.write.save_as_table(dim_fqn, mode="overwrite")
        print(f"DIM_GEOGRAFIA: {count} filas")
