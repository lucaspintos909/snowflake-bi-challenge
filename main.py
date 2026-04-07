import argparse
from utils.session import get_session
from ingestion.load_raw import load_raw
from transform.dim_time import build_dim_tiempo
from transform.dim_geography import build_dim_geografica
from transform.dim_context import build_dim_contexto
from transform.dim_student import build_dim_estudiante
from transform.fact_activity import build_fact_actividad


def run_ingestion(session) -> None:
    print("\n=== CARGA DE DATOS ===")
    load_raw(session)


def run_transforms(session) -> None:
    print("\n=== TRANSFORMACIONES ===")
    build_dim_tiempo(session)
    build_dim_geografica(session)
    build_dim_contexto(session)
    build_dim_estudiante(session)
    build_fact_actividad(session)


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

    session = get_session()
    try:
        if args.ingest_only:
            run_ingestion(session)
        elif args.transform_only:
            run_transforms(session)
        else:
            run_ingestion(session)
            run_transforms(session)
    finally:
        session.close()

    print("\nPipeline completada.")


if __name__ == "__main__":
    main()
