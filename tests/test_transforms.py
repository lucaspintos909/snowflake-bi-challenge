# tests/test_transforms.py
import sys
from pathlib import Path
from unittest.mock import MagicMock


def _make_mock_df(columns):
    df = MagicMock()
    df.select.return_value = df
    df.distinct.return_value = df
    df.sort.return_value = df
    df.with_column.return_value = df
    df.join.return_value = df
    df.count.return_value = 42
    df.write.save_as_table.return_value = None
    df.limit.return_value = df
    df.collect.return_value = [[0]]
    return df


def _setup_snowflake_mocks():
    mock_functions = MagicMock()
    mock_functions.col = MagicMock(return_value=MagicMock())
    mock_functions.row_number = MagicMock(return_value=MagicMock())
    mock_functions.max = MagicMock(return_value=MagicMock())
    mock_functions.lit = MagicMock(return_value=MagicMock())

    mock_window = MagicMock()
    mock_window.Window = MagicMock()
    mock_window.Window.order_by = MagicMock(return_value=MagicMock())

    sys.modules['snowflake'] = MagicMock()
    sys.modules['snowflake.snowpark'] = MagicMock()
    sys.modules['snowflake.snowpark.functions'] = mock_functions
    sys.modules['snowflake.snowpark.window'] = mock_window

    return mock_functions, mock_window


def _make_config():
    from utils.config import PipelineConfig
    return PipelineConfig(
        year=2025,
        csv_path=Path("dummy.csv"),
        csv_delimiter=",",
        csv_columns=[],
        database="CEIBAL_DB",
        raw_schema="RAW",
        mart_schema="MART",
        stage="RAW_STAGE",
    )


def test_build_dim_tiempo_saves_to_correct_table():
    _setup_snowflake_mocks()
    session = MagicMock()
    mock_df = _make_mock_df(["ANIO_LECTIVO"])
    session.table.return_value = mock_df

    config = _make_config()
    from transform.dim_time import build_dim_tiempo
    build_dim_tiempo(session, config)

    assert mock_df.write.save_as_table.called
    call_args = mock_df.write.save_as_table.call_args
    assert "DIM_TIEMPO" in call_args[0][0]


def test_build_dim_geografia_saves_to_correct_table():
    _setup_snowflake_mocks()
    session = MagicMock()
    mock_df = _make_mock_df(["DEPARTAMENTO", "ZONA"])
    session.table.return_value = mock_df

    config = _make_config()
    from transform.dim_geography import build_dim_geografia
    build_dim_geografia(session, config)

    assert mock_df.write.save_as_table.called
    call_args = mock_df.write.save_as_table.call_args
    assert "DIM_GEOGRAFIA" in call_args[0][0]


def test_build_dim_contexto_saves_to_correct_table():
    _setup_snowflake_mocks()
    session = MagicMock()
    mock_df = _make_mock_df(["SUBSISTEMA", "CICLO", "GRADO", "CONTEXTO"])
    session.table.return_value = mock_df

    config = _make_config()
    from transform.dim_context import build_dim_contexto
    build_dim_contexto(session, config)

    assert mock_df.write.save_as_table.called
    call_args = mock_df.write.save_as_table.call_args
    assert "DIM_CONTEXTO" in call_args[0][0]


def test_build_dim_estudiante_saves_to_correct_table():
    _setup_snowflake_mocks()
    session = MagicMock()
    mock_df = _make_mock_df(["ID_PERSONA", "SEXO"])
    session.table.return_value = mock_df

    config = _make_config()
    from transform.dim_student import build_dim_estudiante
    build_dim_estudiante(session, config)

    assert mock_df.write.save_as_table.called
    call_args = mock_df.write.save_as_table.call_args
    assert "DIM_ESTUDIANTE" in call_args[0][0]


def test_build_fact_actividad_saves_to_correct_table():
    session = MagicMock()
    mock_df = _make_mock_df([])
    session.table.return_value = mock_df

    config = _make_config()
    from transform.fact_activity import build_fact_actividad
    build_fact_actividad(session, config)

    assert mock_df.write.save_as_table.called
    call_args = mock_df.write.save_as_table.call_args
    assert "FACT_ACTIVIDAD" in call_args[0][0]
    assert call_args[1]["mode"] == "append"
