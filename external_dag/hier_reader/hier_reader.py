# pylint: disable=logging-fstring-interpolation consider-using-f-string
# pylint: disable=inconsistent-quotes

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
#
"""Creates hierarchy related DAG files and tables."""

# TODO: Make file fully lintable, and remove all pylint disabled flags.

import yaml
import sys
import logging
import os
import datetime

from dag_hierarchies_module import generate_hier
from generate_query import check_create_hiertable, generate_hier_dag_files
from generate_query import copy_to_storage

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not sys.argv[1]:
    raise SystemExit("No Source Project provided")
source_project = sys.argv[1]

if not sys.argv[2]:
    raise SystemExit("No Source Dataset provided")
source_dataset = sys.argv[1] + "." + sys.argv[2]

if not sys.argv[3]:
    raise SystemExit("No Target Project provided")
target_project = sys.argv[3]

if not sys.argv[4]:
    raise SystemExit("No Target Dataset provided")
target_dataset = sys.argv[3] + "." + sys.argv[4]

if not sys.argv[5]:
    raise SystemExit("No GCS bucket provided")
gcs_bucket = sys.argv[5]

os.makedirs("../../generated_dag", exist_ok=True)

client = bigquery.Client()

# Process hierarchies
with open("sets.yaml", encoding="utf-8") as f:
    datasets = yaml.load(f, Loader=yaml.SafeLoader)

    for dataset in datasets["sets_data"]:
        logging.info(f"== Processing dataset {dataset['setname']} ==")
        nodes = []

        full_table = "{tgtd}.{tab}_hier".format(
            tgtd=target_dataset, tab=dataset["table"]
        )

        query = """SELECT  1
             FROM `{src_dataset}.setnode`
             WHERE setname = \'{setname}\'
               AND setclass = \'{setclass}\'
               AND subclass = \'{org_unit}\'
               AND mandt = \'{mandt}\'
               LIMIT 1 """.format(
            src_dataset=source_dataset,
            setname=dataset["setname"],
            mandt=dataset["mandt"],
            setclass=dataset["setclass"],
            org_unit=dataset["orgunit"],
        )

        query_job = client.query(query)

        print(query_job)

        if not query_job:
            logging.info(f"Dataset {dataset['setname']} not found in SETNODES")
            continue

        # Check if table exists, create if not and populate with full initial
        # load.
        try:
            check_create_hiertable(full_table, dataset["key_field"])

            logging.info(f"Generating dag for {full_table}")
            today = datetime.datetime.now()
            substitutes = {
                "setname": dataset["setname"],
                "full_table": full_table,
                "year": today.year,
                "month": today.month,
                "day": today.day,
                "src_project": source_project,
                "src_dataset": source_dataset,
                "setclass": dataset["setclass"],
                "orgunit": dataset["orgunit"],
                "mandt": dataset["mandt"],
                "table": dataset["table"],
                "select_key": dataset["key_field"],
                "where_clause": dataset["where_clause"],
                "load_frequency": dataset["load_frequency"],
            }

            dag_file_name = "cdc_" + full_table.replace(".", "_") + ".py"
            generate_hier_dag_files(dag_file_name, **substitutes)
            generate_hier(**substitutes)

        except NotFound as e:
            # logging, but keep going
            logging.error(f"Table {full_table} not found")

    # Copy template python processor used by all into specific directory
    copy_to_storage(
        gcs_bucket, "dags/hierarchies", "./", "dag_hierarchies_module.py"
    )
