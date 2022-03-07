[![pipeline status](http://git.kittycorns.dev/core-tex-glaring/sap-reporting/badges/main/pipeline.svg)](http://git.kittycorns.dev/core-tex-glaring/sap-reporting/-/commits/main)


# SAP Reporting

Operational reporting templates for Cortex Data Foundation (formerly known as Lucia's views)

This is a list of views that have been tested in a demo environment.

Contents:

[[_TOC_]]


## Variables

This table describe the required variablas and their uses

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

## Testing

This goes over the testing framework for this module.
### Running tests 

Get bats version >= 1.5.0

```shell
mkdir -p tests/test_helper
git clone https://github.com/bats-core/bats-core.git tests/bats                       
git clone https://github.com/bats-core/bats-support.git tests/test_helper/bats-support
git clone https://github.com/bats-core/bats-assert.git tests/test_helper/bats-assert

cd tests/bats
sudo ./install.sh /usr
```

Run the tests

```shell
tests/tests.sh
```

Log files are generated under `tests/logs/{date}.log`

### Adding tests

1. Create a file under `tests/resources`
2. Write your tests in the following format

```sql
---description:title
VALID_ASSERTION_SQL_QUERY
```
The framework expects SQLs to be written as assertions.
The assertions must be prefaced with `---description:` followed by the title of the test cases

#### Examples
Here is an example:
Assume you have the following already created in BigQuery:
```sql
create or replace  table `myprojectid.simpletest.something` (
    id INTEGER,
    name STRING,
)

insert into simpletest.something (id, name) values (1, "blue") ; 
insert into simpletest.something (id, name) values (2, "cat") ; 
insert into simpletest.something (id, name) values (3, "horse") ; 
insert into simpletest.something (id, name) values (4, "sky") ; 
insert into simpletest.something (id, name) values (5, "red") ; 
insert into simpletest.something (id, name) values (6, "green") ; 
insert into simpletest.something (id, name) values (7, "tiger") ; 
```
We can go ahead and create two files with the following contents:
tests/resources/query1.sql
```sql
---description:test1 description
assert ( SELECT id from myprojectid.simpletest.something where name = 'red' ) = 5 ;
---description:test2 with kitties
assert ( SELECT id from myprojectid.simpletest.something where name = 'tiger' ) = 7
```

tests/resources/query2.sql
```sql
---description:test 3 something something
assert (
    SELECT id from
        myprojectid.simpletest.something
    where name = 'sky'
    ) = 4

---description:test 4 
assert (
    SELECT name from
        myprojectid.simpletest.something
    where id = 3
    ) = 'horse'
```

To execute the tests  run the `tests/tests.sh` script

### Parametrization

You should parametrize your tests using jinja syntax. The variables are interpolated 
from the file `tests/resources/data.json` here is an example:
```sql
---description:test2 with kitties
assert ( SELECT id from {{ project }}.{{ dataset }}.something where name = 'tiger' ) = 7
```

This follows the exact same format as the development `data.json` and allows for adding variables that are specific to test cases if needed.
