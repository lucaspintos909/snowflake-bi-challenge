from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session
    from utils.config import PipelineConfig


def build_dim_estudiante(session: Session, config: PipelineConfig) -> None:
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.window import Window

    raw = session.table(config.raw_table_ref)

    # Un estudiante puede aparecer en múltiples años lectivos; se deduplica por ID_PERSONA.
    # SEXO es el único atributo demográfico personal disponible en el dataset.
    dim = (
        raw
        .select("ID_PERSONA", "SEXO")
        .distinct()
        .sort("ID_PERSONA")
    )

    # Surrogate key secuencial ordenada por ID_PERSONA
    window = Window.order_by(F.col("ID_PERSONA"))
    dim = dim.with_column("SK_ESTUDIANTE", F.row_number().over(window))
    dim = dim.select("SK_ESTUDIANTE", "ID_PERSONA", "SEXO")

    count = dim.count()
    dim.write.save_as_table(config.mart_table("DIM_ESTUDIANTE"), mode="overwrite")
    print(f"DIM_ESTUDIANTE: {count} filas")
