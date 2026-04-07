from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session
    from utils.config import PipelineConfig


def build_dim_tiempo(session: Session, config: PipelineConfig) -> None:
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    raw = session.table(config.raw_table_ref)
    dim = (
        raw
        .select(F.col("ANIO_LECTIVO").cast("INTEGER").alias("ANIO_LECTIVO"))
        .distinct()
        .sort("ANIO_LECTIVO")
    )
    window = Window.order_by(F.col("ANIO_LECTIVO"))
    dim = dim.with_column("SK_TIEMPO", F.row_number().over(window))
    dim = dim.select("SK_TIEMPO", "ANIO_LECTIVO")
    count = dim.count()
    dim.write.save_as_table(config.mart_table("DIM_TIEMPO"), mode="overwrite")
    print(f"DIM_TIEMPO: {count} filas")
