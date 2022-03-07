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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.GLAccountsMD`
OPTIONS(
  description = "GL Accounts Master Data"
)
AS
SELECT
  SKA1.MANDT AS Client_MANDT,
  SKA1.KTOPL AS ChartOfAccounts_KTOPL,
  SKA1.SAKNR AS GlAccountNumber_SAKNR,
  SKA1.XBILK AS Indicator_AccountIsABalanceSheetAccount_XBILK,
  SKA1.SAKAN AS GlAccountNumber_SignificantLength_SAKAN,
  SKA1.BILKT AS GroupAccountNumber_BILKT,
  SKA1.ERDAT AS DateOnWhichTheRecordWasCreated_ERDAT,
  SKA1.ERNAM AS NameOfPersonWhoCreatedTheObject_ERNAM,
  SKA1.GVTYP AS PlStatementAccountType_GVTYP,
  SKA1.KTOKS AS GlAccountGroup_KTOKS,
  SKA1.MUSTR AS NumberOfTheSampleAccount_MUSTR,
  SKA1.VBUND AS CompanyIdOfTradingPartner_VBUND,
  SKA1.XLOEV AS Indicator_AccountMarkedForDeletion_XLOEV,
  SKA1.XSPEA AS Indicator_AccountIsBlockedForCreation_XSPEA,
  SKA1.XSPEB AS Indicator_IsAccountBlockedForPosting_XSPEB,
  SKA1.XSPEP AS Indicator_AccountBlockedForPlanning_XSPEP,
  SKA1.MCOD1 AS SearchTermForUsingMatchcode_MCOD1,
  SKA1.FUNC_AREA AS FunctionalArea_FUNC_AREA,
  SKAT.SPRAS AS Language_SPRAS,
  SKAT.TXT20 AS GlAccountShortText_TXT20,
  SKAT.TXT50 AS GlAccountLongText_TXT50,
  SKAT.MCOD1 AS SearchTermForMatchcodeSearch_MCOD1
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.ska1` AS ska1
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.skat` AS skat
  ON ska1.mandt = skat.mandt AND ska1.ktopl = skat.ktopl AND ska1.saknr = skat.saknr
