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
from google.cloud import bigquery
import yaml
import sys

client = bigquery.Client()

def generate_hier(**kwargs):

    src_project = kwargs["src_project"]
    src_dataset = kwargs["src_dataset"]
    mandt = kwargs["mandt"]
    setname = kwargs["setname"]
    setclass = kwargs["setclass"]
    orgunit = kwargs["orgunit"]
    table = kwargs["table"]
    select_key = kwargs["select_key"]
    where_clause = kwargs["where_clause"]
    full_table = kwargs["full_table"]

    nodes = []
    nodes = get_nodes(src_dataset, mandt, setname, 
                        setclass, orgunit, table,
                        select_key, where_clause, full_table)

    if not nodes:
        print("Dataset {setname}  not found in SETNODES".format(setname=setname))
        return
    # ATTENTION - this means the sets file has the root for this specific table
    # and the whole hierarchy will be flattened each time
    # Uncomment if full datasets are created or implement merge
    # trunc_sql = "TRUNCATE TABLE {ft}".format(ft=full_table)
    # errors = client.query(trunc_sql)
    # if errors == []:
    #     print("Truncated table {}".format(full_table))
    # else:
    #     print("Encountered errors while attempting to truncate: {}".format(errors))
    insert_rows(full_table, nodes)


def insert_rows(full_table, nodes):
    errors = client.insert_rows_json(full_table, nodes)
    if errors == []:
        print("New rows added to table {}".format(full_table))
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def get_nodes(src_dataset, mandt, setname, setclass, org_unit, 
              table, select_key, where_clause, full_table):
    sets_tables = []
    query = """SELECT  setname, setclass, subclass, lineid, subsetcls, subsetscls, subsetname
             FROM  `{src_dataset}.setnode`
             WHERE setname = \'{setname}\' 
              AND setclass = \'{setclass}\'
              AND subclass = \'{org_unit}\' 
              AND mandt = \'{mandt}\' """.format(src_dataset=src_dataset,
                                                 setname=setname,
                                                 mandt=mandt,
                                                 setclass=setclass,
                                                 org_unit=org_unit)

    query_job = client.query(query)
    for setr in query_job:
        sets_tables = []
        nodes = get_leafs_children(src_dataset, mandt, setr, table, select_key,
                                   where_clause, full_table)
        print(nodes)
        sets_tables.append(nodes)
        insert_rows(full_table, sets_tables)

    return sets_tables


def get_leafs_children(src_dataset, mandt, row, table, field, where_clause,
                       full_table):
    node_dict = dict()
    # TODO: would be nice to implement multithreaded calls

    node_dict = {
        "mandt": mandt,
        "parent": row['setname'],
        "parent_org": row['subclass'],
        "child": row['subsetname'],
        "child_org": row['subsetscls']
    }

    # Get values from setleaf (only lower child sets have these)
    query = """SELECT valsign, valoption, valfrom, valto
            FROM `{src_dataset}.setleaf`
            WHERE setname = \'{setname}\'
              AND setclass = \'{setclass}\'
              AND subclass = \'{subclass}\'
              AND mandt = \'{mandt}\' """.format(src_dataset=src_dataset,
                                                 setname=row['subsetname'],
                                                 mandt=mandt,
                                                 setclass=row['subsetcls'],
                                                 subclass=row['subsetscls'])

    leafs = client.query(query)

    # Get values from actual master data tables (e.g., Costs center, GL Accounts, etc)

    for setl in leafs:
        # Field = the key (e.g., profit center: CEPC-PRCTC)
        # Where clause parses additional filters: MANDT, Controlling Area, valid-to date
        if setl['valoption'] == 'EQ':
            where_cls = " {field}  = \'{valfrom}\' ".format(
                field=field, valfrom=setl['valfrom'])
        elif setl['valoption'] == 'BT':
            where_cls = " {field} between \'{valfrom}\' and  \'{valto}\' ".format(
                field=field, valfrom=setl['valfrom'], valto=setl['valto'])
        for clause in where_clause:
            where_cls = where_cls + " AND {clause} ".format(clause=clause)

        query = """ SELECT `{field}`
                    FROM `{src_dataset}.{table}`
                    WHERE mandt  = \'{mandt}\'
                      AND {where_cls}""".format(field=field,
                                                src_dataset=src_dataset,
                                                table=table,
                                                mandt=mandt,
                                                where_cls=where_cls)

        ranges = client.query(query)
        for line in ranges:
            node_dict = {
                "mandt": mandt,
                "parent": row['setname'],
                "parent_org": row['subclass'],
                "child": row['subsetname'],
                "child_org": row['subsetscls'],
                field: line[field]
            }
    # Recursive call for child dataset
    get_nodes(src_dataset, mandt, row['subsetname'], row['subsetcls'],
              row['subsetscls'], table, field, where_clause, full_table)
    return node_dict  # This may only have a parent/child
