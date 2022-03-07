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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProductHierarchiesMD`
OPTIONS(
  description = "Product Hierarchies Master Data"
)
AS
SELECT t179.mandt AS Client_MANDT,
  t179.prodh AS Hierarchy_PRODH,
  t179.stufe AS Level_STUFE,
  t179t.spras AS Language_SPRAS,
  t179t.vtext AS Description_VTEXT
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t179` AS t179
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.t179t` AS t179t
  ON t179.mandt = t179t.mandt AND t179.prodh = t179t.prodh
