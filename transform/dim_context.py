from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session
    from utils.config import PipelineConfig


def build_dim_contexto(session: Session, config: PipelineConfig) -> None:
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    raw = session.table(config.raw_table_ref)

    # CONTEXTO es el índice de vulnerabilidad (Quintil 1–5) de DGEIP.
    # Para DGES/DGETP el dataset trae "Sin Dato", lo que es correcto — se mantiene tal cual.
    # La combinación (SUBSISTEMA, CICLO, GRADO, CONTEXTO) describe el tipo de trayectoria educativa.
    dim = (
        raw
        .select("SUBSISTEMA", "CICLO", "GRADO", "CONTEXTO")
        .distinct()
        .sort("SUBSISTEMA", "CICLO", "GRADO", "CONTEXTO")
    )

    # Surrogate key secuencial sobre la combinación única
    window = Window.order_by(
        F.col("SUBSISTEMA"), F.col("CICLO"), F.col("GRADO"), F.col("CONTEXTO")
    )
    dim = dim.with_column("SK_CONTEXTO", F.row_number().over(window))
    dim = dim.select("SK_CONTEXTO", "SUBSISTEMA", "CICLO", "GRADO", "CONTEXTO")

    count = dim.count()
    dim.write.save_as_table(config.mart_table("DIM_CONTEXTO"), mode="overwrite")
    print(f"DIM_CONTEXTO: {count} filas")
