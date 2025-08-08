# Add New Data Source to Warehouse

This Dataform model is responsible for the `T` in `ELT`:

> Dataform lets you manage data transformation in the Extraction, Loading, and Transformation (ELT) process for data integration. After raw data is extracted from source systems and loaded into BigQuery, Dataform helps you to transform it into a well-defined, tested, and documented suite of data tables.

New sources of data will need to be extracted and loaded into BigQuery so they can be used in the data warehouse.

## Extracting and Loading Data

Your goal when extracting and loading data from third-party datas sources should be getting the fields you need as easily and cheaply as possible.  If you are accessing a relatively popular data source, you should not have to create a custome extraction tool.

Look for pre-built solutions in the following order:

1.  Is there a BigQuery Data Transfer source?
2.  Is there a feature in the source itself to export data (such as CSVs) to a cloud storage bucket?
3.  Is there a free airbyte connector?
4.  Is there a paid connector in something like fivetran or Supermetrics?
5.  If nothing else can be used, build a custom extraction tool either as a cloud function, cloud run, or airbyte connector. 


## Creating a new source config in Dataform

[Declare a data source](https://cloud.google.com/dataform/docs/declare-source)

1. In the `definitions/sources` directory, create a new SQLX file for your data source declaration:  `source_name.sqlx`.
2. Add the configuration object:

    ```js
    config {
    type: "declaration",
    database: "DATABASE",
    schema: "SCHEMA",
    name: "NAME",
    }
    ```

    `schema` is the BigQuery dataset name and `name` is the table name.
3. Create a staging model to `SELECT` the source data.