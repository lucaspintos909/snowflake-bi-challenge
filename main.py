import argparse
from pathlib import Path

from utils.config import PipelineConfig
from utils.session import get_session
from ingestion.load_raw import load_raw
from transform.dim_time import build_dim_tiempo
from transform.dim_geography import build_dim_geografica
from transform.dim_context import build_dim_contexto
from transform.dim_student import build_dim_estudiante
from transform.fact_activity import build_fact_actividad

CONFIG_PATH = Path(__file__).parent / "pipeline_config.yaml"


def run_ingestion(session, config: PipelineConfig) -> None:
    print("\n=== CARGA DE DATOS ===")
    load_raw(session, config)


def run_transforms(session, config: PipelineConfig) -> None:
    print("\n=== TRANSFORMACIONES ===")
    build_dim_tiempo(session, config)
    build_dim_geografica(session, config)
    build_dim_contexto(session, config)
    build_dim_estudiante(session, config)
    build_fact_actividad(session, config)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ceibal ELT Pipeline — Snowflake + Snowpark"
    )
    parser.add_argument(
        "--ingest-only", action="store_true", help="Solo corre el paso de ingesta"
    )
    parser.add_argument(
        "--transform-only", action="store_true", help="Solo corre las transformaciones"
    )
    args = parser.parse_args()

    config = PipelineConfig.from_yaml(CONFIG_PATH)
    session = get_session()
    session.sql(f"USE DATABASE {config.database}").collect()
    try:
        if args.ingest_only:
            run_ingestion(session, config)
        elif args.transform_only:
            run_transforms(session, config)
        else:
            run_ingestion(session, config)
            run_transforms(session, config)
    finally:
        session.close()

    print("\nPipeline completada.")


if __name__ == "__main__":
    main()
