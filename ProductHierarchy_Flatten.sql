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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProductHierarchy_Flatten`
OPTIONS(
  description = "Product Hierarchy Flattener"
)
AS
WITH h1_h2 AS (
  SELECT h1.prodh AS prodh1, h2.prodh AS prodh2
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t179` AS h1
  LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.t179` AS h2
    ON starts_with(h2.prodh, h1.prodh)
  WHERE h1.stufe = '1'
    AND h2.stufe = '2'
)
SELECT h1_h2.prodh1 AS prodh1,
  h1_h2.prodh2 AS prodh2,
  h3.prodh AS prodh3
FROM h1_h2 AS h1_h2
LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.t179` AS h3
  ON starts_with(h3.prodh, h1_h2.prodh2)
WHERE h3.stufe = '3'
