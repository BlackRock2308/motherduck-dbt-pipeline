from setuptools import find_packages, setup

setup(
    name="dbt_dagster_immo",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "dbt_dagster_immo": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-snowflake<1.10",
        "dbt-duckdb<1.10",
        "dbt-duckdb<1.10",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)