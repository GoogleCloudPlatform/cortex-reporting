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
CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ dataset_cdc_processed }}.csks_hier` (
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
SELECT h.parent AS ParentHierarchy, h.child AS ChildHierarchy,
  ht.descript AS Description, pmd.CostCenter_KOSTL, pmd.Description_LTEXT,
  b.AmountInLocalCurrency_DMBTR, b.AmountInDocumentCurrency_WRBTR
-- cepc_hier is created as part of the hierarchy flattening of cost centers during CDC deployment 
-- or as part of the deployment of this sample so deployment does not fail
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.csks_hier` AS h
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.setheadert` AS ht
  ON h.mandt = ht.mandt
    AND h.child_org = ht.subclass
    AND h.child = ht.setname
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCentersMD` AS pmd
  ON h.mandt = pmd.Client_MANDT AND h.kostl = pmd.CostCenter_KOSTL AND ht.langu = pmd.Language_SPRAS
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountingDocuments` AS b
  ON h.mandt = b.Client_MANDT AND h.kostl = b.CostCenter_KOSTL
WHERE ht.langu = 'E' AND ht.setclass = '0101';
