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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.LedgersMD`
OPTIONS(
  description = "General Ledger Master Data"
)
AS
SELECT
  t881.mandt AS Client_MANDT,
  t881.rldnr AS Ledger_RLDNR,
  t881.gcurr AS Currency_GCURR,
  t881.class AS CompanyCurrencyRole_CLASS,
  t881.logsys AS LogicalSystem_LOGSYS,
  t881.valutyp AS ControllingValuationType_VALUTYP,
  t881.xleading AS LeadingLedgerFlag_XLEADING,
  t881.appl AS LedgerApplication_APPL,
  t881.subappl AS LedgerSubapplication_SUBAPPL,
  t881.curt1 AS AddlCurrRole1_CURT1,
  t881.curt2 AS AddlCurrRole2_CURT2,
  t881.curt3 AS AddlCurrRole3_CURT3,
  t881t.langu AS Language_LANGU,
  t881t.name AS Name
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t881` AS t881
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.t881t` AS t881t
  ON t881.rldnr = t881t.rldnr AND t881.mandt = t881t.mandt
