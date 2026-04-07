from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session
    from utils.config import PipelineConfig


def build_dim_tiempo(session: Session, config: PipelineConfig) -> None:
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    raw = session.table(f"{config.raw_schema}.{config.raw_table}")
    candidates = (
        raw
        .select(F.col("ANIO_LECTIVO").cast("INTEGER").alias("ANIO_LECTIVO"))
        .distinct()
    )

    dim_fqn = f"{config.mart_schema}.DIM_TIEMPO"

    try:
        existing = session.table(dim_fqn)
        existing.limit(0).collect()
        max_sk = existing.select(F.max(F.col("SK_TIEMPO"))).collect()[0][0] or 0
        nuevas = candidates.join(
            existing.select("ANIO_LECTIVO"), on="ANIO_LECTIVO", how="left_anti"
        )
        if nuevas.count() == 0:
            print("DIM_TIEMPO: sin cambios")
            return
        window = Window.order_by(F.col("ANIO_LECTIVO"))
        nuevas = nuevas.with_column("SK_TIEMPO", F.lit(max_sk) + F.row_number().over(window))
        nuevas = nuevas.select("SK_TIEMPO", "ANIO_LECTIVO")
        count = nuevas.count()
        nuevas.write.save_as_table(dim_fqn, mode="append")
        print(f"DIM_TIEMPO: +{count} filas nuevas")
    except Exception:
        window = Window.order_by(F.col("ANIO_LECTIVO"))
        dim = candidates.with_column("SK_TIEMPO", F.row_number().over(window))
        dim = dim.select("SK_TIEMPO", "ANIO_LECTIVO")
        count = dim.count()
        dim.write.save_as_table(dim_fqn, mode="overwrite")
        print(f"DIM_TIEMPO: {count} filas")
