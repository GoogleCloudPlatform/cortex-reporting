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
  CEPC.MANDT AS Client_MANDT,
  CEPC.PRCTR AS ProfitCenter_PRCTR,
  CEPC.DATBI AS ValidToDate_DATBI,
  CEPC.KOKRS AS ControllingArea_KOKRS,
  CEPC.DATAB AS ValidFromDate_DATAB,
  CEPC.ERSDA AS CreatedOn_ERSDA,
  CEPC.USNAM AS EnteredBy_USNAM,
  CEPC.MERKMAL AS FieldNameOfCoPaCharacteristic_MERKMAL,
  CEPC.ABTEI AS Department_ABTEI,
  CEPC.VERAK AS PersonResponsibleForProfitCenter_VERAK,
  CEPC.VERAK_USER AS UserResponsibleForTheProfitCenter_VERAK_USER,
  CEPC.WAERS AS CurrencyKey_WAERS,
  CEPC.NPRCTR AS SuccessorProfitCenter_NPRCTR, CEPC.LAND1 AS CountryKey_LAND1,
  CEPC.ANRED AS Title_ANRED, CEPC.NAME1 AS NAME1, CEPC.NAME2 AS NAME2,
  CEPC.NAME3 AS NAME3, CEPC.NAME4 AS NAME4, CEPC.ORT01 AS City_ORT01,
  CEPC.ORT02 AS District_ORT02,
  CEPC.STRAS AS StreetAndHouseNumber_STRAS, CEPC.PFACH AS PoBox_PFACH,
  CEPC.PSTLZ AS PostalCode_PSTLZ, CEPC.PSTL2 AS POBoxPostalCode_PSTL2,
  CEPC.SPRAS AS CtrLanguage_SPRAS,
  CEPC.TELBX AS TeleboxNumber_TELBX, CEPC.TELF1 AS FirstTelephoneNumber_TELF1,
  CEPC.TELF2 AS SecondTelephoneNumber_TELF2, CEPC.TELFX AS FaxNumber_TELFX,
  CEPC.TELTX AS TeletexNumber_TELTX, CEPC.TELX1 AS TelexNumber_TELX1,
  CEPC.DATLT AS DataCommunicationLineNo_DATLT,
  CEPC.DRNAM AS PrinterNameForProfitCenter_DRNAM,
  CEPC.KHINR AS ProfitCenterArea_KHINR,
  CEPC.BUKRS AS CompanyCode_BUKRS,
  CEPC.VNAME AS JointVenture_VNAME,
  CEPC.RECID AS RecoveryIndicator_RECID, CEPC.ETYPE AS EquityType_ETYPE,
  CEPC.TXJCD AS TaxJurisdiction_TXJCD, CEPC.REGIO AS Region_state_REGIO,
  CEPC.KVEWE AS UsageOfTheConditionTable_KVEWE,
  CEPC.KAPPL AS Application_KAPPL, CEPC.KALSM AS Procedure__pricing__KALSM,
  CEPC.LOCK_IND AS LockIndicator_LOCK_IND,
  CEPC.PCA_TEMPLATE AS TemplateForFormulaPlanningInProfitCenters_PCA_TEMPLATE,
  CEPC.SEGMENT AS SegmentForSegmentalReporting_SEGMENT,
  CEPCT.SPRAS AS Language_SPRAS, CEPCT.DATBI AS TxtValidToDate_DATBI,
  CEPCT.KTEXT AS GeneralName_KTEXT,
  CEPCT.LTEXT AS LongText_LTEXT,
  CEPCT.MCTXT AS SearchTermForMatchcodeSearch_MCTXT
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.cepc` AS cepc
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.cepct` AS cepct
  ON cepc.mandt = cepct.mandt
    AND cepc.prctr = cepct.prctr
    AND cepc.datbi = cepct.datbi AND cepc.kokrs = cepct.kokrs
WHERE cast(cepc.datab AS STRING) <= concat( cast(extract( YEAR FROM current_date())AS STRING), cast(extract( MONTH FROM current_date())AS STRING), cast(extract( DAY FROM current_date())AS STRING) )
  AND cast(cepc.datbi AS STRING) >= concat( cast(extract( YEAR FROM current_date())AS STRING), cast(extract( MONTH FROM current_date())AS STRING), cast(extract( DAY FROM current_date())AS STRING) )
  