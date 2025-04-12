from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import immobilier_courtage_project


@dbt_assets(manifest=immobilier_courtage_project.manifest_path)
def immobilier_courtage_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    