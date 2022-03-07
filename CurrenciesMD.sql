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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CurrenciesMD`
OPTIONS(
  description = "Currencies Master Data"
)
AS
SELECT
  tcurc.mandt AS Client_MANDT, tcurc.waers AS CurrencyCode_WAERS, tcurc.isocd AS CurrencyISO_ISOCD,
  tcurx.currdec AS CurrencyDecimals_CURRDEC, tcurt.spras AS Language,
  tcurt.ktext AS CurrShortText_KTEXT, tcurt.ltext AS CurrLongText_LTEXT
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurc` AS tcurc
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurx` AS tcurx ON tcurc.waers = tcurx.currkey
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurt`AS tcurt
  ON tcurc.waers = tcurt.waers AND tcurc.mandt = tcurt.mandt
