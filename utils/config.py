from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class PipelineConfig:
    csv_path: Path
    database: str
    raw_schema: str
    mart_schema: str
    stage: str
    raw_table: str

    @property
    def raw_table_fqn(self) -> str:
        """Nombre completo para DDL/COPY INTO: DATABASE.SCHEMA.TABLE"""
        return f"{self.database}.{self.raw_schema}.{self.raw_table}"

    @property
    def raw_table_ref(self) -> str:
        """Referencia en dos partes para session.table(): SCHEMA.TABLE"""
        return f"{self.raw_schema}.{self.raw_table}"

    @property
    def stage_fqn(self) -> str:
        return f"{self.database}.{self.raw_schema}.{self.stage}"

    def mart_table(self, table: str) -> str:
        """Referencia en dos partes para una tabla MART: SCHEMA.TABLE"""
        return f"{self.mart_schema}.{table}"

    @classmethod
    def from_yaml(cls, path: Path) -> "PipelineConfig":
        import yaml

        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        csv_section = data["csv"]
        sf = data["snowflake"]
        return cls(
            csv_path=Path(csv_section["path"]),
            database=sf["database"],
            raw_schema=sf["raw_schema"],
            mart_schema=sf["mart_schema"],
            stage=sf["stage"],
            raw_table=sf["raw_table"],
        )
