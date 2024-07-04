# ELT Data pipeline using Prefect, Python, Motherduck, DuckDB

## About the Project
This project will extract and load sales data into motherduck and will then be transformed using duckdb SQL.

## Directories
`csv_files` - contains the `states.csv` file with states data extracted from rapidAPI

`dbt_md_product_sales` - contains dbt files.
> This project currently uses duckdbSQL to transform the data. Dbt adapter for motherduck has some connection issues.

`eda` - contains eda_sales.ipynb which is used for data cleaning and transformation locally.

`img` - contains image files

`prefect-flows` - contains all python scripts used for extraction and orchestration.


## Project Diagram
