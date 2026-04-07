from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class PipelineConfig:
    year: int
    csv_path: Path
    csv_delimiter: str
    csv_columns: list[str]
    database: str
    raw_schema: str
    mart_schema: str
    stage: str

    @property
    def raw_table(self) -> str:
        return f"ACTIVIDAD_ESTUDIANTES_{self.year}"

    @property
    def raw_table_fqn(self) -> str:
        return f"{self.database}.{self.raw_schema}.{self.raw_table}"

    @property
    def stage_fqn(self) -> str:
        return f"{self.database}.{self.raw_schema}.{self.stage}"

    @classmethod
    def from_yaml(cls, path: Path) -> "PipelineConfig":
        import yaml

        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        csv_section = data["csv"]  # raises KeyError if missing
        sf_section = data["snowflake"]

        return cls(
            year=int(data["year"]),
            csv_path=Path(csv_section["path"]),
            csv_delimiter=csv_section["delimiter"],
            csv_columns=csv_section["columns"],
            database=sf_section["database"],
            raw_schema=sf_section["raw_schema"],
            mart_schema=sf_section["mart_schema"],
            stage=sf_section["stage"],
        )
