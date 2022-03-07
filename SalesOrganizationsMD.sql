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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesOrganizationsMD`
OPTIONS(
  description = "Sales Organizations Master Data"
)
AS
SELECT
  tvko.mandt AS Client_MANDT,
  tvko.vkorg AS SalesOrg_VKORG,
  tvko.waers AS SalesOrgCurrency_WAERS,
  tvko.kunnr AS SalesOrgCustomer_KUNNR,
  tvko.bukrs AS CompanyCode_BUKRS,
  t001.land1 AS Country_LAND1,
  t001.waers AS CoCoCurrency_WAERS,
  t001.periv AS FiscalYrVariant_PERIV,
  t001.butxt AS Company_BUTXT,
  tvkot.vtext AS SalesOrgName_VTEXT,
  tvkot.spras AS Language_SPRAS
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.tvko` AS tvko
LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.t001` AS t001
  ON tvko.mandt = t001.mandt AND tvko.bukrs = t001.bukrs
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.tvkot` AS tvkot
  ON tvko.mandt = tvkot.mandt AND tvko.vkorg = tvkot.vkorg
