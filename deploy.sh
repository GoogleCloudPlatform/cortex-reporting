#!/bin/bash
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Script to call cloudbuild.reporting.yaml which in turn calls view.sh

#--------------------
# Help Message
#--------------------
usage() {
  cat <<HELP_USAGE

Triggers deployment of reporting views for SAP REPORTING.

$0 [OPTIONS]

Options
-h | --help                     : Display this message
-a | source-project             : Source Dataset Project ID. Mandatory
-b | target-project             : Target Dataset Project ID. Mandatory
-x | cdc-processed-dataset      : Source Dataset Name. Mandatory
-y | raw-landing-dataset        : Raw Landing Dataset Name. (Default: cdc-processed-dataset)
-c | cdc-processed-dataset-ecc  : Source Dataset Name ECC. Mandatory
-d | cdc-processed-dataset-s4   : Source Dataset Name S4. Mandatory
-g | raw-landing-dataset-ecc    : Raw Landing Dataset Name. (Default: cdc-processed-dataset-ecc)
-i | raw-landing-dataset-s4     : Raw Landing Dataset Name. (Default: cdc-processed-dataset-s4)
-r | target-reporting-dataset   : Target Dataset Name for Reporting (Default: REPORTING)
-s | target-models-dataset      : Target Dataset Name for ML (Default: MODELS)
-l | location                   : Dataset Location (Default: US)
-m | mandt                      : SAP Mandant. (Default: 100)
-n | mandt-ecc                  : SAP Mandant for ECC. (Default: 100)
-o | mandt-s4                   : SAP Mandant for S4. (Default: 200)
-f | sql-flavour                : SQL Flavor Selection, ECC or S4 or UNION. Mandatory
-j | language                   : Language(s) using SAP codes (Default: 'E', can accept multiple values)
-k | currency                   : Currency using SAP codes (Default: 'USD', can accept multiple values)

HELP_USAGE

}

err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z') ERROR]: $*" >&2
}

#--------------------
# Validate input
#--------------------
validate() {

  PJID_SRC="${project_id_src:-${PJID_SRC}}"
  PJID_TGT="${project_id_tgt:-${PJID_TGT}}"
  DS_CDC="${dataset_cdc_processed:-${DS_CDC}}"
  DS_RAW="${dataset_raw_landing:-${DS_RAW}}"
  DS_CDC_ECC="${dataset_cdc_processed_ecc:-${DS_CDC_ECC}}"
  DS_CDC_S4="${dataset_cdc_processed_s4:-${DS_CDC_S4}}"
  DS_RAW_ECC="${dataset_raw_landing_ecc:-${DS_RAW_ECC}}"
  DS_RAW_S4="${dataset_raw_landing_s4:-${DS_RAW_S4}}"
  DS_REPORTING="${dataset_reporting_tgt:-${DS_REPORTING}}"
  DS_MODELS="${dataset_models_tgt:-${DS_MODELS}}"
  LOCATION="${location:-${LOCATION}}"
  SQL_FLAVOUR="${sql_flavour:-${SQL_FLAVOUR}}"
  SQL_FLAVOUR=$(echo "${SQL_FLAVOUR}" | tr '[:upper:]' '[:lower:]')
  echo "${SQL_FLAVOUR}"
  LANGUAGE="${default_language:-${LANGUAGE}}"
  CURRENCY="${default_currency:-${CURRENCY}}"
  MANDT="${mandt:-${MANDT}}"
  MANDT_ECC="${mandt_ecc:-${MANDT_ECC}}"
  MANDT_S4="${mandt_ecc:-${MANDT_S4}}"

  if [ -z "${PJID_SRC-}" ]; then
    err '"source-project" is required. See help for details.'
    exit 1
  fi

  if [ -z "${PJID_TGT-}" ]; then
    echo 'INFO: "target-project" missing, defaulting to source-project.'
    PJID_TGT="${PJID_SRC}"
  fi

  ## For backwards compatibility, when UNION was not an option, dataset_cdc_processed and dataset_raw_landing
  ## should be taken into account. These values can come from a parameter (e.g., dataset_cdc_processed) or from an env variable
  ## populated by sap_config.env (e.g, DS_CDC_ECC). Parameters provided to the script override the config file
  case "${SQL_FLAVOUR}" in
    "ecc")
      #Flavour specific (-ecc) may not be provided. Use default (dataset_cdc_processed/DS_CDC) for backwards compatibility
      dataset_cdc_processed_ecc="${dataset_cdc_processed_ecc:-"${dataset_cdc_processed}"}"
      DS_CDC_ECC="${DS_CDC_ECC:-"${DS_CDC}"}"
      # If a value comes from a parameter, it should override sap_config.env
      DS_CDC_ECC="${dataset_cdc_processed_ecc:-"${DS_CDC_ECC}"}"

      # If flavor-specific is not provided, try backwards-compatible
      dataset_raw_landing_ecc="${dataset_raw_landing_ecc:-"${dataset_raw_landing}"}"
      DS_RAW_ECC="${DS_RAW_ECC:-"${DS_RAW}"}"
      DS_RAW_ECC="${dataset_raw_landing_ecc:-"${DS_RAW_ECC}"}"

      if [[ -z "${DS_RAW_ECC}" ]] || [[ -z "${DS_CDC_ECC}" ]]; then
        err 'CDC or RAW dataset missing. Please provide a parameter or fill in sap_config.env'
        exit 1
      fi

      mandt_ecc="${mandt_ecc:-${mandt}}"
      MANDT_ECC="${mandt_ecc:-${MANDT_ECC}}"

      if [[ -z "${MANDT_ECC-}" ]]; then
        err 'MANDT missing. Please provide a parameter or fill in sap_config.env'
        exit 1
      fi
    ;;
    "s4")
      #Flavour specific (-s4) may not be provided. Use default (dataset_cdc_processed/DS_CDC) for backwards compatibility
      dataset_cdc_processed_s4="${dataset_cdc_processed_s4:-"${dataset_cdc_processed}"}"
      DS_CDC_S4="${DS_CDC_S4:-"${DS_CDC}"}"
      # If a value comes from a parameter, it should override sap_config.env
      DS_CDC_S4="${dataset_cdc_processed_s4:-"${DS_CDC_S4}"}"

      # If flavor-specific is not provided, try backwards-compatible
      dataset_raw_landing_s4="${dataset_raw_landing_s4:-"${dataset_raw_landing}"}"
      DS_RAW_S4="${DS_RAW_S4:-"${DS_RAW}"}"
      DS_RAW_S4="${dataset_raw_landing_s4:-"${DS_RAW_S4}"}"

      if [[ -z "${DS_RAW_S4}" ]] || [[ -z "${DS_CDC_S4}" ]]; then
        err 'CDC or RAW dataset missing. Please provide a parameter or fill in sap_config.env'
        exit 1
      fi

      mandt_s4="${mandt_s4:-${mandt}}"
      MANDT_S4="${mandt_S4:-${MANDT_S4}}"

      if [[ -z "${MANDT_S4}" ]]; then
        err 'MANDT missing. Please provide a parameter or fill in sap_config.env'
        exit 1
      fi
    ;;

    "union")
      # Needs all SQL flavor-specific values
      # If a value comes from a parameter, it should override sap_config.env
      DS_CDC_ECC="${dataset_cdc_processed_ecc:-"${DS_CDC_ECC}"}"
      DS_RAW_ECC="${dataset_raw_landing_ecc:-"${DS_RAW_ECC}"}"
      if [[ -z "${DS_RAW_ECC}" ]] || [[ -z "${DS_CDC_ECC}" ]]; then
        err 'ECC CDC or RAW dataset missing. Please provide a parameter or fill in config.env'
        exit 1
      fi

      DS_CDC_S4="${dataset_cdc_processed_s4:-"${DS_CDC_S4}"}"
      DS_RAW_S4="${dataset_raw_landing_s4:-"${DS_RAW_S4}"}"
      if [[ -z "${DS_RAW_S4}" ]] || [[ -z "${DS_CDC_S4}" ]]; then
        err 'S4 CDC or RAW dataset missing. Please provide a parameter or fill in sap_config.env'
        exit 1
      fi
      MANDT_S4="${mandt_S4:-${MANDT_S4}}"
      MANDT_ECC="${mandt_ecc:-${MANDT_ECC}}"
      if [[ -z "${MANDT_S4}" || -z "${MANDT_ECC}" ]]; then
        err 'MANDT for ECC or S4 missing. Please provide a parameter or fill in sap_config.env'
        exit 1
      fi

    ;;
    *)
      echo "Invalid option ($1) for SQL flavor. Run --help for usage" >&2
      exit 1
    ;;
  esac

  if [[ -z "${DS_REPORTING}" ]]; then
    echo 'INFO: "target-reporting-dataset" missing, defaulting to REPORTING.'
    DS_REPORTING="REPORTING"
  fi

  if [[ -z "${DS_MODELS}" ]]; then
    echo 'INFO: "target-models-dataset" missing, defaulting to ML_MODELS.'
    DS_MODELS="ML_MODELS"
  fi

  if [[ -z "${LOCATION}" ]]; then
    echo 'INFO: "location" missing, defaulting to US.'
    LOCATION="US"
  fi

  if [[ -z "${LANGUAGE}" ]]; then
    echo 'INFO: "language" is missing, defaulting to "E".'
    LANGUAGE="= 'E' "
  else
    if [[ "${LANGUAGE}" =~ .*','.* ]]; then
      LANGUAGE="in ( ${LANGUAGE} )"
    else
      LANGUAGE="= ${LANGUAGE}"
    fi
  fi

  if [[ -z "${CURRENCY}" ]]; then
    echo 'INFO: "default_currency" is missing, defaulting to "USD".'
    CURRENCY="= 'USD'"
  else
    if [[ ${CURRENCY} =~ .*','.* ]]; then
      CURRENCY="in ( ${CURRENCY} )"
    else
      CURRENCY="= ${CURRENCY}"
    fi
  fi

  if [[ ! -f "dependencies_${SQL_FLAVOUR}.txt" || ! -s "dependencies_${SQL_FLAVOUR}.txt" ]]; then
    err "Did not find dependencies file for ${SQL_FLAVOUR}"
    exit 1
  fi

}

#--------------------
# Make Safe for bq mk
#--------------------

bq_safe_mk() {
  dataset=$1
  # Ideally, this would work, but bq ignores the location flag
  #exists=$(bq ls -d | grep -w "$dataset")
  #if [ -n "$exists" ]; then
  exists=$(bq query --location=$LOCATION --project_id=$PJID_TGT --use_legacy_sql=false "select distinct 'KITTYCORN' from ${dataset}.INFORMATION_SCHEMA.TABLES")
  if [[ "$exists" == *"KITTYCORN"* ]]; then
    echo "Not creating $dataset since it already exists"
  else
    echo "Creating dataset $PJID_TGT:$dataset with location: $LOCATION"
    bq --location="$LOCATION" mk --dataset "$PJID_TGT:$dataset"
  fi
}

#--------------------
# Parameters parsing
#--------------------

set -o errexit -o noclobber -o nounset -o pipefail
params="$(getopt -o ha:b:x:y:c:d:g:i:r:s:l:m:n:o:f:j:k: -l help,source-project:,target-project:,cdc-processed-dataset:,raw-landing-dataset:,cdc-processed-dataset-ecc:,cdc-processed-dataset-s4:,raw-landing-dataset-ecc:,raw-landing-dataset-s4:,target-reporting-dataset:,target-models-dataset:,location:,mandt:,mandt-ecc:,mandt-s4:,sql-flavour:,language:,currency: --name "$0" -- "$@")"
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
    -y | --raw-landing-dataset)
      dataset_raw_landing=$2
      shift 2
      ;;
    -c | --cdc-processed-dataset-ecc)
      dataset_cdc_processed_ecc=$2
      shift 2
      ;;
    -d | --cdc-processed-dataset-s4)
      dataset_cdc_processed_s4=$2
      shift 2
      ;;
    -g | --raw-landing-dataset-ecc)
      dataset_raw_landing_ecc=$2
      shift 2
      ;;
    -i | --raw-landing-dataset-s4)
      dataset_raw_landing_s4=$2
      shift 2
      ;;
    -r | --target-reporting-dataset)
      dataset_reporting_tgt=$2
      shift 2
      ;;
    -s | --target-models-dataset)
      dataset_models_tgt=$2
      shift 2
      ;;
    -l | --location)
      location=$2
      shift 2
      ;;
    -m | --mandt)
      mandt=$2
      shift 2
      ;;
    -n | --mandt-ecc)
      mandt_ecc=$2
      shift 2
      ;;
    -o | --mandt-s4)
      mandt_s4=$2
      shift 2
      ;;
    -f | --sql-flavour)
      sql_flavour=$2
      shift 2
      ;;
    -j | --language)
      default_language=$2
      shift 2
      ;;
    -k | --currency)
      default_currency=$2
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

# views_dir=${@:$OPTIND:1}

set +o errexit +o noclobber +o nounset +o pipefail

#--------------------
# Main logic
#--------------------

if [ -f sap_config.env ]; then
    set -a
    source <(cat ./sap_config.env | sed -e 's/^[ \t]*//;s/[ \t]*$//;/^#/d;/^\s*$/d;s#\([^\]\)"#\1#g;s/=\(.*\)/=\"\1\"/g;s/^/export /;s/$/;/')
    set +a
else
    echo "+++ WARNING: sap_config.env +++ not found"
fi

echo -e "\n🦄🦄🦄 Validating parameters for Google \x1b[38;2;66;133;244mCloud \x1b[38;2;234;67;53mCortex \x1b[38;2;251;188;5mData \x1b[38;2;52;168;83mFoundation\x1b[0m 🔪🔪🔪\n"

validate

# helpful for debugging
echo "Running with the following parameters:"
echo "source-project: ${PJID_SRC}"
echo "target-project: ${PJID_TGT}"
echo "cdc-processed-dataset: ${DS_CDC}"
echo "raw-landing-dataset: ${DS_RAW}"
echo "cdc-processed-dataset-ecc: ${DS_CDC_ECC}"
echo "cdc-processed-dataset-s4: ${DS_CDC_S4}"
echo "raw-landing-dataset-ecc: ${DS_RAW_ECC}"
echo "raw-landing-dataset-s4: ${DS_RAW_S4}"
echo "target-reporting-dataset: ${DS_REPORTING}"
echo "target-models-dataset: ${DS_MODELS}"
echo "location: ${LOCATION}"
echo "mandt: ${MANDT}"
echo "mandt-s4: ${MANDT_S4}"
echo "mandt-ecc: ${MANDT_ECC}"
echo "sql-flavour: ${SQL_FLAVOUR}"
echo "currency: ${CURRENCY}"
echo "language: ${LANGUAGE}"
echo "Turbo mode: ${TURBO}"


success=0

bq_safe_mk "${DS_REPORTING}"
bq_safe_mk "${DS_MODELS}"

echo -e "\n🦄🦄🦄 Generating json file 🔪🔪🔪\n"

cat <<EOF >view.json
{
  "project_id_src": "${PJID_SRC}",
  "project_id_tgt": "${PJID_TGT}",
  "dataset_raw_landing_ecc": "${DS_RAW_ECC}",
  "dataset_raw_landing_s4": "${DS_RAW_S4}",
  "dataset_cdc_processed_ecc": "${DS_CDC_ECC}",
  "dataset_cdc_processed_s4": "${DS_CDC_S4}",
  "dataset_reporting_tgt": "${DS_REPORTING}",
  "dataset_models_tgt": "${DS_MODELS}",
  "mandt": "${MANDT}",
  "mandt_s4": "${MANDT_S4}",
  "mandt_ecc": "${MANDT_ECC}",
  "sql_flavour": "${SQL_FLAVOUR}",
  "currency": "${CURRENCY}",
  "language": "${LANGUAGE}"
}
EOF

echo -e "🦄🦄🦄 Generating views deployment 🔪🔪🔪"

if [[ "${TURBO}" == true ]]; then

  no_wait="waitFor: ['-']"
  step_counter=0
  build_file_counter=0

  rm -f cloudbuild.views-*.yaml

  while read -r file_entry; do
    # if delimiter
    if [[ "${file_entry}" == *"----"* || -z "${file_entry}"  ]]
    then
      no_wait=" "
      continue
    fi

    build_file_number=$(printf "%02d" ${build_file_counter})
    build_file_name="cloudbuild.views-${build_file_number}.yaml"

    if [[ "${step_counter}" == "0" ]]
    then
      cat cloudbuild.views.start.yaml >> "${build_file_name}"
    fi

    cat cloudbuild.views.step.yaml | \
      sed "s/_SQL_FILE_NAME_HERE_/${file_entry}/g" | \
      sed "s/_NO_WAIT_HERE_/${no_wait}/g" >> "${build_file_name}"

    step_counter=$((step_counter+1))

    if [[ "${step_counter}" == "99" ]]
    then
      cat cloudbuild.views.end.yaml >> "${build_file_name}"
      step_counter=0
      build_file_counter=$((build_file_counter+1))
    fi

  done <"dependencies_${SQL_FLAVOUR}.txt"

  if [[ "${step_counter}" != "0" ]]
  then
    cat cloudbuild.views.end.yaml >> "${build_file_name}"
  fi

  echo -e "\n🦄🦄🦄 Executing views deployment in TURBO💨 mode 😺😺😺"

  for phase in $( seq 0 $build_file_counter )
  do
    build_file_number=$(printf "%02d" ${phase})
    build_file_name="cloudbuild.views-${build_file_number}.yaml"
    if [[ -f "${build_file_name}" ]]
    then
      echo -e "\nRunning build ${build_file_name}..."
      gcloud builds submit . --config="${build_file_name}" --substitutions=_LOCATION="${LOCATION}",_SQL_FLAVOUR="${SQL_FLAVOUR}"
      if [ $? = 1 ]; then
        success=1
      fi
    fi
  done
else
  while read -r file_entry; do
    # if delimiter or empty lines
    if [[ "${file_entry}" == *"----"* || -z "${file_entry}"  ]]; then
      continue
    fi
    echo "Creating ${file_entry} in sequential mode 😺😺😺"
    gcloud builds submit . --config=./cloudbuild.view.yaml --substitutions=_LOCATION="${LOCATION}",_SQL_FLAVOUR="${SQL_FLAVOUR}",_SQL_FILE="${file_entry}"
    # Substitutions replaced by config.json file
    #,_PJID_SRC="${project_id_src}",_PJID_TGT="${project_id_tgt}",_DS_CDC="${dataset_cdc_processed}",_DS_RAW="${dataset_raw_landing}",_DS_REPORTING="${dataset_reporting_tgt}",_DS_MODELS="${dataset_models_tgt}",_MANDT="${mandt}",_LOCATION="${location}",_SQL_FLAVOUR="${sql_flavour}" .

    if [ $? = 1 ]; then
      success=1
    fi
  done <"dependencies_${SQL_FLAVOUR}.txt"
fi

exit "${success}"
