#!/bin/bash

EXTERNAL_DAGS=("currency_conversion" "prod_hierarchy_texts" "inventory_snapshots")
REPORTING_DAGS=("currency_conversion" "prod_hierarchy_texts" "inventory_snapshots")

#--------------------
# Help Message
#--------------------
usage() {
  cat <<HELP_USAGE

Generate exernal DAG files (currency_conversion, inventory snapshots, etc.).

$0 [OPTIONS]

Options
-h | --help                       : Display this message
-a | --source-project             : Source Dataset Project ID. Mandatory
-b | --target-project             : Target Dataset Project ID. Mandatory
-x | --cdc-processed-dataset      : Source Dataset Name. Default: CDC_PROCESSED
-r | --reporting-dataset          : Reporting Dataset Name. Default: REPORTING
-k | --k9-processing-dataset      : K9 processing dataset. Madatory.
-l | --location                   : BigQuery dataset location. Default US
-t | --test-data                  : Populate with test data. Default false
-s | --run-ext-sql                : Run external DAGs SQLs Default: true
-f | --sql-flavour                : S4 or ECC flavour
-m | --mandt                      : MANDT (Client Id)

HELP_USAGE

}

#--------------------
# Validate input
#--------------------
validate() {

  if [ -z "${project_id_src-}" ]; then
    echo 'ERROR: "source-project" is required. See help for details.'
    exit 1
  fi

  if [ -z "${project_id_tgt-}" ]; then
    echo 'ERROR: "target-project" is required. See help for details.'
    exit 1
  fi

  if [ -z "${run_ext_sql-}" ]; then
    echo 'INFO: External DAGs SQL files will be executed.'
    run_ext_sql="true"
  fi

  if [ -z "${test_data-}" ]; then
    echo 'INFO: test data will not be loaded.'
    test_data="false"
  fi

  if [ -z "${location-}" ]; then
    echo 'INFO: "location" not provided. Defaulting to US.'
    location="US"
  fi

  if [ -z "${dataset_cdc_processed-}" ]; then
    echo 'INFO: "cdc-processed-dataset" not provided, defaulting to CDC_PROCESSED.'
    dataset_cdc_processed="CDC_PROCESSED"
  fi

  if [ -z "${dataset_reporting-}" ]; then
    echo 'INFO: "reporting-dataset" not provided, defaulting to REPORTING.'
    dataset_reporting="REPORTING"
  fi

  if [ -z "${k9_datasets_processing-}" ]; then
    echo 'ERROR: "k9-processing-dataset" is required. See help for details.'
    exit 1
  fi


  if [ -z "${sql_flavour-}" ]; then
    echo 'INFO: "sql_flavour" not provided. Defaulting to ecc.'
    sql_flavour="ecc"
  fi

  if [ -z "${mandt-}" ]; then
    echo 'INFO: "mandt" not provided. Defaulting to 100.'
    mandt="100"
  fi

  exists=$(bq query --location="${location}" --project_id="${project_id_src}" --use_legacy_sql=false "select distinct 'KITTYCORN' from \`"${dataset_cdc_processed}".INFORMATION_SCHEMA.TABLES\`")
  if [[ ! "$exists" == *"KITTYCORN"* ]]; then
    echo "ERROR: Dataset $dataset_cdc_processed does not exist or has no tables, Aborting."
    exit 1
  fi

  echo "Trying to create Reporting Dataset $dataset_reporting if it does not exist..."
  bq show "${project_id_tgt}:${dataset_reporting}" > /dev/null || bq --location="${location}" mk --dataset "${project_id_tgt}:${dataset_reporting}"
  echo "Done."

}

#--------------------
# Parameters parsing
#--------------------

set -o errexit -o noclobber -o nounset -o pipefail
params="$(getopt -o ha:b:x:l:t:s:f:m:r:k: -l help,source-project:,target-project:,cdc-processed-dataset:,location:,test-data:,run-ext-sql:,sql-flavour:,mandt:,reporting-dataset:,k9-processing-dataset: --name "$0" -- "$@")"
eval set -- "$params"

while true; do
  case "$1" in
  -h | --help)
    usage
    shift
    exit
    ;;
  -a | --source-project)
    project_id_src=$2
    shift 2
    ;;
  -b | --target-project)
    project_id_tgt=$2
    shift 2
    ;;
  -x | --cdc-processed-dataset)
    dataset_cdc_processed=$2
    shift 2
    ;;
  -r | --reporting-dataset)
    dataset_reporting=$2
    shift 2
    ;;
  -k | --k9-processing-dataset)
    k9_datasets_processing=$2
    shift 2
    ;;
  -l | --location)
    location=$2
    shift 2
    ;;
  -t | --test-data)
    test_data="${2}"
    shift 2
    ;;
  -s | --run-ext-sql)
    run_ext_sql="${2}"
    shift 2
    ;;
  -f | --sql-flavour)
    sql_flavour="${2}"
    shift 2
    ;;
  -m | --mandt)
    mandt="${2}"
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *)
    echo "Invalid option ($1). Run --help for usage" >&2
    exit 1
    ;;
  esac
done

set +o errexit +o noclobber +o nounset +o pipefail

#--------------------
# Main logic
#--------------------

validate

success=0

# Force lowercase to ensure Jinja replaces correctly
lowflavour=$(echo "${sql_flavour}" | tr '[:upper:]' '[:lower:]')

cat <<EOF >data.json
{
  "project_id_src": "${project_id_src}",
  "dataset_cdc_processed": "${dataset_cdc_processed}",
  "project_id_tgt": "${project_id_tgt}",
  "k9_datasets_processing": "${k9_datasets_processing}",
  "dataset_reporting_tgt": "${dataset_reporting}",
  "sql_flavour": "${lowflavour}",
  "mandt": "${mandt}"
}
EOF

##
# assumption is that all files are in a folder with the same name as the deployment configuration
# e.g. "holiday" folder contains all holiday dag files etc
# the DAGS should feed and clean the data coming from the APIs - so there's technically no RAW_LANDING
##

mkdir -p generated_dag
mkdir -p generated_sql

lowcation=$(echo "${location}" | tr '[:upper:]' '[:lower:]')

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
common_dir="${SCRIPT_DIR}/common"
export PYTHONPATH="${PYTHONPATH}:${SCRIPT_DIR}"
if [[ -f "config/config.json" ]]; then
  config_file="config/config.json"
else
  config_file="${SCRIPT_DIR}/config/sap_config.json"
fi
test_harness_project=$(jq -r ."testDataProject" "${config_file}")
test_harness_dataset=$(python3 -c "from common.py_libs.test_harness import get_test_harness_dataset; print(get_test_harness_dataset('SAP','reporting','${lowcation}'))")

for dag in "${EXTERNAL_DAGS[@]}"; do
  echo "INFO: checking for external DAG $dag"
  if [ -d "external_dag/${dag}" ]; then

    echo "INFO: Found, creating external DAG ${dag}"
    paths=(external_dag/"${dag}"/*)
    # Temporary fix to check Reporting dataset for Reporting DAGs
    if [[ " ${REPORTING_DAGS[*]} " =~ " ${dag} " ]]; then
      target_project_id=${project_id_tgt}
      target_dataset_name=${dataset_reporting}
    else
      target_project_id=${project_id_src}
      target_dataset_name=${dataset_cdc_processed}
    fi

    for p in "${paths[@]}"; do
      echo "INFO: processing file ${p}"
      file=$(basename "${p}")

      if [[ $p = *.py ]]; then
        cp "${p}" "generated_dag/${file}"
      fi

      if [[ $p = *.ini ]]; then
        jinja -d data.json "${p}" >"generated_dag/${file}"
        if [ $? = 1 ]; then success=1; fi
      fi

      if [[ $p = *.templatesql ]]; then
        jinja -d data.json "${p}" >"generated_sql/${file%.*}.sql"
        if [ $? = 1 ]; then success=1; fi
      fi

      if [[ $p = *.sql ]]; then
        query=$(jinja -d data.json "${p}")
        echo "${query}" >"generated_sql/${file}"
        if [[ "${run_ext_sql}" == "true" ]]; then
          bq query --batch -sync=true --project_id="${target_project_id}" --location="${location}" --use_legacy_sql=false "${query}"
          _sql_code=$?
        else
          echo "${file} will not be executed (--run-ext-sql is false)."
          _sql_code=1
        fi
        if [ $_sql_code -ne 0 ] && [[ "${test_data}" != "true" ]]; then
          if [[ "${run_ext_sql}" != "true" ]]; then
            echo "${file} was not executed (--run-ext-sql is false) and --test-data is ${test_data}. This is unusual, but ok."
          else
            echo "ERROR: ${file} execution was not successful and --test-data is false."
            success=1
          fi
        else
          if [[ "${test_data}" == "true" ]]; then
            table_name="${file%.*}"
            echo "Processing test data for ${table_name}"
            num_rows_str=$(bq query --location="${location}" --project_id="${target_project_id}" \
              --use_legacy_sql=false --format=csv --quiet \
              "SELECT COUNT(*) FROM \`${target_dataset_name}.${table_name}\`")
            if [[ $? -ne 0 ]]; then
              num_rows=0
            else
              num_rows=$(echo -e "${num_rows_str}" | tail -1)
            fi
            if [ "$num_rows" -eq 0 ]; then
              echo "INFO: Loading test data for ${table_name} from ${test_harness_project}:${test_harness_dataset}.${table_name}"
              bq cp --location="${lowcation}" --project_id "${target_project_id}" --force=true --headless=true \
                ${test_harness_project}:${test_harness_dataset}.${table_name} ${target_project_id}:${target_dataset_name}.${table_name}
              if [[ $? = 1 ]]; then success=1; fi
            else
              echo "INFO: Skipping loading of test data for $table_name as it already has data"
            fi
          fi
        fi
      fi
    done

  else
    echo "External dag ${dag} not found"
    success=1
  fi

done

exit "${success}"
