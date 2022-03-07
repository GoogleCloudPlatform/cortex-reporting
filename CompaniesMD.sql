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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD`
OPTIONS(
  description = "Company Master Data"
)
AS
SELECT
  t001.mandt AS Client_MANDT, t001.waers AS CurrencyCode_WAERS,
  t001.bukrs AS CompanyCode_BUKRS, t001.butxt AS CompanyText_BUTXT,
  t001.ort01 AS CityName_ORT01, t001.LAND1 AS Country_LAND1,
  t001.SPRAS AS Language, t001.KTOPL AS ChartOfAccounts_KTOPL,
  t001.PERIV AS FiscalyearVariant_PERIV, t001.RCOMP AS Company_RCOMP, t001.KKBER AS CreditControlArea_KKBER,
  T001.KTOP2 AS CountryChartofAccounts_KTOP2, T001.FIKRS AS FundsManagement_FIKRS, tka02.kokrs AS ControllingArea_KOKRS
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t001` AS t001
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.tka02` AS tka02
  ON t001.mandt = tka02.mandt AND t001.bukrs = tka02.bukrs
