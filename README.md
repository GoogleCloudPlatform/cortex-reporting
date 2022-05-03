# SAP Reporting

Templates for SAP reporting (and more!) for Google Cloud Cortex Data Foundation

## Deployment

We recommend looking at the instructions in the parent module, the [Cortex Data Foundation](https://github.com/GoogleCloudPlatform/cortex-data-foundation). You will find instructions and details on the parameters for the deployment there.

Individual views can be deployed with `cloudbuild.reporting.yaml` using the same parameters as described in the parent module. 

## Variables

This table describe the required variables for the Jinja templates and their uses

| Name                  | Description | Mandatory | Default Value |
|-----------------------|-------------|-----------|---------------|
| `project_id_src`        | Source Google Cloud Project:<br /> Project where the source data is located which the data models will consume. | Y | N/A 
| `project_id_tgt`        | Target Google Cloud Project:<br /> Project where Data Foundation for SAP predefined data models will be deployed and accessed by end-users. <br /> This may or may not be different from the source project. | Y | N/A 
| `dataset_raw_landing`   | Source BigQuery Dataset:<br /> BigQuery dataset where the source SAP data is replicated to or where the test data will be created.  | Y | N/A 
| `dataset_cdc_processed` | CDC BigQuery Dataset:<br /> BigQuery dataset where the CDC processed data lands the latest available records. <br /> This may or may not be the same as the source dataset.  | Y | N/A
| `dataset_reporting_tgt` | Target BigQuery reporting dataset:<br /> BigQuery dataset where the Data Foundation for SAP predefined data models will be deployed. | N | SAP_REPORTING 
| `dataset_models_tgt`    | Target BigQuery reporting dataset:<br /> BigQuery dataset where the Data Foundation for SAP predefined data models will be deployed. | N | SAP_ML_MODELS 
| `mandt`                 | SAP Mandant. Must be 3 character.  | Y | 800
| `sql_flavour`           | Which database target type. <br />Valid values are `ECC` or `S4` | N | `ECC`



## Simple local output

If you want to test the output of the jinja template locally you can use `jinja-cli` for a quick check:

1. First install jinja-cli:
```shell
pip install jinja-cli
```

2. Then create a json file with the required input data:
```shell
cat  <<EOF > data.json
  "project_id_src": "your-source-project",
  "project_id_tgt": "your-target-project",
  "dataset_raw_landing": "your-raw-dataset",
  "dataset_cdc_processed": "your-cdc-processed-dataset",
  "dataset_reporting_tgt": "your-reporting-target-dataset-OR-SAP_REPORTING",
  "dataset_models_tgt": "your-mlmodels-target-dataset-OR-ML_MODELS",
  "mandt": "your-mandt-number-800",
  "sql_flavour": "ECC"
}
EOF
```

Here is what an example looks like
```json
{
  "project_id_src": "kittycorn-dev",
  "project_id_tgt": "kittycorn-dev",
  "dataset_raw_landing": "ECC_REPL",
  "dataset_cdc_processed": "CDC_PROCESSED",
  "dataset_reporting_tgt": "SAP_REPORTING",
  "dataset_models_tgt": "ML_MODELS",
  "mandt": "800",
  "sql_flavour": "ECC"
}
```

3. Create an output folder
```shell
mkdir output
```

4. Now generate the parsed file:

```shell
jinja -d data.json -o ouput/filename.sql filename.sql
```

Alternatively, if you want to generate all files:
```
for f in *.sql; do
    echo "processing $f ..."
    jinja -d data.json -o "output/${f}" "${f}"
done
```
