# Customer 360 real-time processing built for db t deployment

This project is a port of the [../c360_flink_processing/](../c360_flink_processing/) project.

## Automatic migration

* Use the tool in flink-studies

```sh
cd tools
uv run flink_dbt_migrate/migrate_dml_to_dbt.py migrate-sl-folder  ~/Documents/Code/flink_project_demos/customer_360/c360_flink_processing/pipelines ~/Documents/Code/flink_project_demos/customer_360/c360_flink_dsp_dbt/pipelines --write --force
```

The seeds have a problem as the dbt adapter does not support nested subtype like ROW.


