from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import immobilier_courtage_dbt_assets
from .project import immobilier_courtage_project
from .schedules import schedules

defs = Definitions(
    assets=[immobilier_courtage_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=immobilier_courtage_project),
    },
)