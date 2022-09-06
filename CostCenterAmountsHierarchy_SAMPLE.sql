#-- Copyright 2022 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

{% if sql_flavour == 'ecc' -%}
 CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.csks_hier` (
  mandt STRING,
  parent STRING,
  parent_org STRING,
  child STRING,
  child_org STRING,
  kostl STRING );
  CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterAmountsHierarchy_SAMPLE`
OPTIONS(
description = "Sample with cost centers and amount represented in flattened hierarchy"
)
AS
({% include './ecc/CostCenterAmountsHierarchy_SAMPLE.sql' -%})
{% endif -%}

{% if sql_flavour == 's4' -%}
CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.csks_hier` (
  mandt STRING,
  parent STRING,
  parent_org STRING,
  child STRING,
  child_org STRING,
  kostl STRING );
  CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterAmountsHierarchy_SAMPLE`
OPTIONS(
description = " Sample with cost centers and amount represented in flattened hierarchy "
)
AS
({% include './s4/CostCenterAmountsHierarchy_SAMPLE.sql' -%})
{% endif -%}
;
