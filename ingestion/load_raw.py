from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session

STAGE_NAME = "RAW_STAGE"
RAW_TABLE = "CEIBAL_DB.RAW.ACTIVIDAD_ESTUDIANTES"
DEFAULT_CSV = Path(__file__).parent.parent / "actividad-de-estudiantes-2025.csv"


def load_raw(session: Session, csv_path: Path = DEFAULT_CSV) -> int:
    session.sql("CREATE SCHEMA IF NOT EXISTS CEIBAL_DB.RAW").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS CEIBAL_DB.MART").collect()
    session.sql(
        f"CREATE OR REPLACE STAGE CEIBAL_DB.RAW.{STAGE_NAME}"
    ).collect()

    put_result = session.file.put(
        str(csv_path.resolve()),
        f"@CEIBAL_DB.RAW.{STAGE_NAME}",
        overwrite=True,
        auto_compress=True,
    )
    print(f"Subida a stage: {[r.status for r in put_result]}")

    session.sql(f"""
        COPY INTO {RAW_TABLE}
        FROM @CEIBAL_DB.RAW.{STAGE_NAME}
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            ENCODING = 'UTF8'
            NULL_IF = ('')
        )
        PURGE = FALSE
    """).collect()

    count = session.table(RAW_TABLE).count()
    print(f"Se cargaron {count:,} filas en {RAW_TABLE}")
    return count
