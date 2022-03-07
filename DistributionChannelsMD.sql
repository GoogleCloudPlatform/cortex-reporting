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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCentersMD`
OPTIONS(
  description = "Profit Centers Master Data"
)
AS
SELECT
  TVTW.MANDT AS Client_MANDT,
  TVTW.VTWEG AS DistributionChannel_VTWEG,
  TVTWT.SPRAS AS LanguageKey_SPRAS,
  TVTWT.VTEXT AS Name_VTEXT
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.tvtw` AS tvtw
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.tvtwt` AS tvtwt
  ON tvtw.mandt = tvtwt.mandt AND tvtw.vtweg = tvtwt.vtweg
