from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session
    from utils.config import PipelineConfig


def load_raw(session: Session, config: PipelineConfig) -> int:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {config.database}.{config.raw_schema}").collect()
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {config.database}.{config.mart_schema}").collect()
    session.sql(f"CREATE OR REPLACE STAGE {config.stage_fqn}").collect()

    put_result = session.file.put(
        str(config.csv_path.resolve()),
        f"@{config.stage_fqn}",
        overwrite=True,
        auto_compress=True,
    )
    print(f"Subida a stage: {[r.status for r in put_result]}")

    session.sql(f"""
        COPY INTO {config.raw_table_fqn}
        FROM @{config.stage_fqn}
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            ENCODING = 'UTF8'
            NULL_IF = ('')
        )
        PURGE = FALSE
    """).collect()

    count = session.table(config.raw_table_ref).count()
    print(f"Se cargaron {count:,} filas en {config.raw_table_fqn}")
    return count
