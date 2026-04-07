from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session
    from utils.config import PipelineConfig


def load_raw(session: Session, config: PipelineConfig) -> int:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {config.database}.{config.raw_schema}").collect()
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {config.database}.{config.mart_schema}").collect()
    session.sql(f"CREATE OR REPLACE STAGE {config.stage_fqn}").collect()

    # Crear tabla RAW del año si no existe
    cols_ddl = ",\n    ".join(f"{col} VARCHAR" for col in config.csv_columns)
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.raw_table_fqn} (
            {cols_ddl}
        )
    """).collect()

    put_result = session.file.put(
        str(config.csv_path.resolve()),
        f"@{config.stage_fqn}",
        overwrite=True,
        auto_compress=True,
    )
    print(f"Subida a stage: {[r.status for r in put_result]}")

    # Mapeo posicional: $1, $2, ... → nombres de columna del config
    cols_list = ", ".join(config.csv_columns)
    positions = ", ".join(f"${i + 1}" for i in range(len(config.csv_columns)))

    session.sql(f"""
        COPY INTO {config.raw_table_fqn} ({cols_list})
        FROM (SELECT {positions} FROM @{config.stage_fqn})
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_DELIMITER = '{config.csv_delimiter}'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            ENCODING = 'UTF8'
            NULL_IF = ('', 'NA')
        )
        PURGE = FALSE
    """).collect()

    count = session.table(config.raw_table_fqn).count()
    print(f"Se cargaron {count:,} filas en {config.raw_table_fqn}")
    return count
