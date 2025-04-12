from pathlib import Path

from dagster_dbt import DbtProject

immobilier_courtage_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "immobilier_courtage").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
immobilier_courtage_project.prepare_if_dev()