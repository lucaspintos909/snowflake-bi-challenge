# tests/test_ingestion.py
from unittest.mock import MagicMock
from pathlib import Path


def test_load_raw_creates_stage_and_copies(tmp_path):
    csv = tmp_path / "test.csv"
    csv.write_text("col1;col2\nval1;val2\n")

    session = MagicMock()
    session.file.put.return_value = [MagicMock(status="UPLOADED")]
    session.table.return_value.count.return_value = 1

    from utils.config import PipelineConfig
    config = PipelineConfig(
        year=2024,
        csv_path=csv,
        csv_delimiter=";",
        csv_columns=["COL1", "COL2"],
        database="CEIBAL_DB",
        raw_schema="RAW",
        mart_schema="MART",
        stage="RAW_STAGE",
    )

    from ingestion.load_raw import load_raw
    count = load_raw(session, config)

    sql_calls = [str(c) for c in session.sql.call_args_list]
    assert any("CREATE SCHEMA IF NOT EXISTS CEIBAL_DB.RAW" in c for c in sql_calls)
    assert any("CREATE SCHEMA IF NOT EXISTS CEIBAL_DB.MART" in c for c in sql_calls)
    assert any("CREATE OR REPLACE STAGE" in c for c in sql_calls)
    assert any("CREATE TABLE IF NOT EXISTS" in c for c in sql_calls)
    assert any("COPY INTO" in c for c in sql_calls)
    assert any("ACTIVIDAD_ESTUDIANTES_2024" in c for c in sql_calls)
    session.file.put.assert_called_once()
    assert count == 1


def test_pipeline_config_from_yaml(tmp_path):
    yaml_content = """
year: 2025
csv:
  path: actividad-de-estudiantes-2025.csv
  delimiter: ","
  columns:
    - ID_PERSONA
    - SEXO
snowflake:
  database: CEIBAL_DB
  raw_schema: RAW
  mart_schema: MART
  stage: RAW_STAGE
"""
    config_file = tmp_path / "2025.yaml"
    config_file.write_text(yaml_content)

    from utils.config import PipelineConfig
    config = PipelineConfig.from_yaml(config_file)

    assert config.year == 2025
    assert config.csv_delimiter == ","
    assert config.csv_columns == ["ID_PERSONA", "SEXO"]
    assert config.raw_table == "ACTIVIDAD_ESTUDIANTES_2025"
    assert config.raw_table_fqn == "CEIBAL_DB.RAW.ACTIVIDAD_ESTUDIANTES_2025"
    assert config.stage_fqn == "CEIBAL_DB.RAW.RAW_STAGE"
    assert config.mart_schema == "MART"


def test_pipeline_config_missing_field_raises(tmp_path):
    yaml_content = """
year: 2025
csv:
  delimiter: ","
  columns: []
snowflake:
  database: CEIBAL_DB
  raw_schema: RAW
  mart_schema: MART
  stage: RAW_STAGE
"""
    config_file = tmp_path / "bad.yaml"
    config_file.write_text(yaml_content)

    from utils.config import PipelineConfig
    import pytest
    with pytest.raises(KeyError):
        PipelineConfig.from_yaml(config_file)
