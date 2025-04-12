{% docs __overview__ %}
# Real Estate Brokerage Pipeline

Welcome to the documentation for our Real Estate Brokerage data pipeline!

This project demonstrates an end-to-end data engineering pipeline, leveraging **MotherDuck** for data storage, **dbt** for data transformation, and **Streamlit** for analytics and visualization. The pipeline follows a **medallion architecture** to organize data into structured layers:

- **Bronze Layer**: Raw data ingestion from CSV files.
- **Silver Layer**: Cleaned and enriched data.
- **Gold Layer**: Aggregated and analytics-ready data for dashboards.

Here is the architecture of our project:
# ![Input Schema](assets/archi.jpeg)

Key features of this pipeline include:
- Client segmentation based on age, income, and employment type.
- Performance metrics for banking partners, including average loan amounts and interest rates.
- Conversion rate analysis by acquisition source.

Explore the models and transformations in this project to understand how we process and analyze real estate brokerage data.

{% enddocs %}