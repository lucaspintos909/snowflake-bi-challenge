from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session
    from utils.config import PipelineConfig


def build_dim_contexto(session: Session, config: PipelineConfig) -> None:
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    raw = session.table(f"{config.raw_schema}.{config.raw_table}")
    candidates = (
        raw
        .select("SUBSISTEMA", "CICLO", "GRADO", "CONTEXTO")
        .distinct()
    )

    dim_fqn = f"{config.mart_schema}.DIM_CONTEXTO"
    natural_keys = ["SUBSISTEMA", "CICLO", "GRADO", "CONTEXTO"]

    try:
        existing = session.table(dim_fqn)
        existing.limit(0).collect()
        max_sk = existing.select(F.max(F.col("SK_CONTEXTO"))).collect()[0][0] or 0
        nuevas = candidates.join(
            existing.select(natural_keys), on=natural_keys, how="left_anti"
        )
        if nuevas.count() == 0:
            print("DIM_CONTEXTO: sin cambios")
            return
        window = Window.order_by(
            F.col("SUBSISTEMA"), F.col("CICLO"), F.col("GRADO"), F.col("CONTEXTO")
        )
        nuevas = nuevas.with_column("SK_CONTEXTO", F.lit(max_sk) + F.row_number().over(window))
        nuevas = nuevas.select("SK_CONTEXTO", *natural_keys)
        count = nuevas.count()
        nuevas.write.save_as_table(dim_fqn, mode="append")
        print(f"DIM_CONTEXTO: +{count} filas nuevas")
    except Exception:
        window = Window.order_by(
            F.col("SUBSISTEMA"), F.col("CICLO"), F.col("GRADO"), F.col("CONTEXTO")
        )
        dim = candidates.with_column("SK_CONTEXTO", F.row_number().over(window))
        dim = dim.select("SK_CONTEXTO", *natural_keys)
        count = dim.count()
        dim.write.save_as_table(dim_fqn, mode="overwrite")
        print(f"DIM_CONTEXTO: {count} filas")
