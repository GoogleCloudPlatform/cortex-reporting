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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.POOrderHistory`
OPTIONS(
  description = "Purchase Documents - Order History"
)
AS

WITH
tcurx AS (
  -- Joining to this table is necesssary to fix the decimal place of 
  -- amounts for non-decimal-based currencies. SAP stores these amounts 
  -- offset by a factor  of 1/100 within the system (FYI this gets 
  -- corrected when a user observes these in the GUI) Currencies w/ 
  -- decimals are unimpacted.
  --
  -- Example of impacted currencies JPY, IDR, KRW, TWD 
  -- Example of non-impacted currencies USD, GBP, EUR
  -- Example 1,000 JPY will appear as 10.00 JPY
  SELECT DISTINCT
    CURRKEY,
    CAST(POWER(10, 2 - COALESCE(CURRDEC, 0)) AS NUMERIC) AS CURRFIX
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurx`
),


conv AS (
  -- This table is used to convert rates from the transaction currency to USD.
  SELECT DISTINCT
    mandt,
    fcurr,
    tcurr,
    ukurs,
    PARSE_DATE("%Y%m%d", CAST(99999999 - CAST(gdatu AS INT64) AS STRING)) AS gdatu
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurr`
  WHERE
    mandt = '{{ mandt }}'
    AND kurst = 'M' -- Daily Corporate Rate
    AND tcurr = 'USD' -- Convert to USD

  UNION ALL

  -- USD to USD rates do not exist in TCURR (or any other rates that are same-to-
  -- same such as EUR to EUR / JPY to JPY etc.
  SELECT
    '{{ mandt }}' AS mandt,
    'USD' AS fcurr,
    'USD' AS tcurr,
    1 AS ukurs,
    date_array AS gdatu
  FROM
    UNNEST(GENERATE_DATE_ARRAY('1990-01-01', '2060-12-31')) AS date_array
)

SELECT
  PO.VendorAccountNumber_LIFNR,
  EKBE.MANDT AS Client_MANDT,
  EKBE.EBELN AS PurchasingDocumentNumber_EBELN,
  EKBE.EBELP AS ItemNumberOfPurchasingDocument_EBELP,
  EKBE.ZEKKN AS SequentialNumberOfAccountAssignment_ZEKKN,
  EKBE.VGABE AS TransactioneventType_VGABE,
  EKBE.GJAHR AS MaterialDocumentYear_GJAHR,
  EKBE.BELNR AS NumberOfMaterialDocument_BELNR,
  EKBE.BUZEI AS ItemInMaterialDocument_BUZEI,
  EKBE.BEWTP AS PurchaseOrderHistoryCategory_BEWTP,
  EKBE.BWART AS MovementType__inventoryManagement___BWART,
  EKBE.BUDAT AS PostingDateInTheDocument_BUDAT,
  EKBE.MENGE AS Quantity_MENGE,
  EKBE.BPMNG AS QuantityInPurchaseOrderPriceUnit_BPMNG,
  EKBE.DMBTR AS AmountInLocalCurrency_DMBTR,
  EKBE.WRBTR AS AmountInDocumentCurrency_WRBTR,
  EKBE.WAERS AS CurrencyKey_WAERS,
  EKBE.AREWR AS GrirAccountClearingValueInLocalCurrency_AREWR,
  EKBE.WESBS AS GoodsReceiptBlockedStockInOrderUnit_WESBS,
  EKBE.BPWES AS QuantityInGrBlockedStockInOrderPriceUnit_BPWES,
  EKBE.SHKZG AS DebitcreditIndicator_SHKZG,
  EKBE.BWTAR AS ValuationType_BWTAR,
  EKBE.ELIKZ AS deliveryCompleted_ELIKZ,
  EKBE.XBLNR AS ReferenceDocumentNumber_XBLNR,
  EKBE.LFGJA AS FiscalYearOfAReferenceDocument_LFGJA,
  EKBE.LFBNR AS DocumentNoOfAReferenceDocument_LFBNR,
  EKBE.LFPOS AS ItemOfAReferenceDocument_LFPOS,
  EKBE.GRUND AS ReasonForMovement_GRUND,
  EKBE.CPUDT AS DayOnWhichAccountingDocumentWasEntered_CPUDT,
  EKBE.CPUTM AS TimeOfEntry_CPUTM,
  EKBE.REEWR AS InvoiceValueEntered__inLocalCurrency___REEWR,
  EKBE.EVERE AS ComplianceWithShippingInstructions_EVERE,
  EKBE.REFWR AS InvoiceValueInForeignCurrency_REFWR,
  EKBE.MATNR AS MaterialNumber_MATNR,
  EKBE.WERKS AS Plant_WERKS,
  EKBE.XWSBR AS ReversalOfGrAllowedForGrBasedIvDespiteInvoice_XWSBR,
  EKBE.ETENS AS SequentialNumberOfVendorConfirmation_ETENS,
  EKBE.KNUMV AS NumberOfTheDocumentCondition_KNUMV,
  EKBE.MWSKZ AS TaxOnSalespurchasesCode_MWSKZ,
  EKBE.LSMNG AS QuantityInUnitOfMeasureFromDeliveryNote_LSMNG,
  EKBE.LSMEH AS UnitOfMeasureFromDeliveryNote_LSMEH,
  EKBE.EMATN AS MaterialNumber_EMATN,
  EKBE.AREWW AS ClearingValueOnGrirClearingAccount__transacCurrency___AREWW,
  EKBE.HSWAE AS LocalCurrencyKey_HSWAE,
  EKBE.BAMNG AS Quantity_BAMNG,
  EKBE.CHARG AS BatchNumber_CHARG,
  EKBE.BLDAT AS DocumentDateInDocument_BLDAT,
  EKBE.XWOFF AS CalculationOfValOpen_XWOFF,
  EKBE.XUNPL AS UnplannedAccountAssignmentFromInvoiceVerification_XUNPL,
  EKBE.ERNAM AS NameOfPersonWhoCreatedTheObject_ERNAM,
  EKBE.SRVPOS AS ServiceNumber_SRVPOS,
  EKBE.PACKNO AS PackageNumberOfService_PACKNO,
  EKBE.INTROW AS LineNumberOfService_INTROW,
  EKBE.BEKKN AS NumberOfPoAccountAssignment_BEKKN,
  EKBE.LEMIN AS ReturnsIndicator_LEMIN,
  EKBE.AREWB AS ClearingValueOnGrirAccountInPoCurrency_AREWB,
  EKBE.REWRB AS InvoiceAmountInPoCurrency_REWRB,
  EKBE.SAPRL AS SapRelease_SAPRL,
  EKBE.MENGE_POP AS Quantity_MENGE_POP,
  EKBE.BPMNG_POP AS QuantityInPurchaseOrderPriceUnit_BPMNG_POP,
  EKBE.DMBTR_POP AS AmountInLocalCurrency_DMBTR_POP,
  EKBE.WRBTR_POP AS AmountInDocumentCurrency_WRBTR_POP,
  EKBE.WESBB AS ValuatedGoodsReceiptBlockedStockInOrderUnit_WESBB,
  EKBE.BPWEB AS QuantityInValuatedGrBlockedStockInOrderPriceUnit_BPWEB,
  EKBE.WEORA AS AcceptanceAtOrigin_WEORA,
  EKBE.AREWR_POP AS GrirAccountClearingValueInLocalCurrency_AREWR_POP,
  EKBE.KUDIF AS ExchangeRateDifferenceAmount_KUDIF,
  EKBE.RETAMT_FC AS RetentionAmountInDocumentCurrency_RETAMT_FC,
  EKBE.RETAMT_LC AS RetentionAmountInCompanyCodeCurrency_RETAMT_LC,
  EKBE.RETAMTP_FC AS PostedRetentionAmountInDocumentCurrency_RETAMTP_FC,
  EKBE.RETAMTP_LC AS PostedSecurityRetentionAmountInCompanyCodeCurrency_RETAMTP_LC,
  EKBE.XMACC AS MultipleAccountAssignment_XMACC,
  EKBE.WKURS AS ExchangeRate_WKURS,
  EKBE.INV_ITEM_ORIGIN AS OriginOfAnInvoiceItem_INV_ITEM_ORIGIN,
  EKBE.VBELN_ST AS Delivery_VBELN_ST,
  EKBE.VBELP_ST AS DeliveryItem_VBELP_ST,
  EKBE.SGT_SCAT AS StockSegment_SGT_SCAT,
  EKBE.ET_UPD AS ProcedureForUpdatingTheScheduleLineQuantity_ET_UPD,
  EKBE.J_SC_DIE_COMP_F AS DepreciationCompletionFlag_J_SC_DIE_COMP_F,
  -- EKBE.FSH_SEASON_YEAR AS SeasonYear_FSH_SEASON_YEAR,
  -- EKBE.FSH_SEASON AS Season_FSH_SEASON,
  -- EKBE.FSH_COLLECTION AS FashionCollection_FSH_COLLECTION,
  -- EKBE.FSH_THEME AS FashionTheme_FSH_THEME,
  EKBE.WRF_CHARSTC1 AS CharacteristicValue1_WRF_CHARSTC1,
  EKBE.WRF_CHARSTC2 AS CharacteristicValue2_WRF_CHARSTC2,
  EKBE.WRF_CHARSTC3 AS CharacteristicValue3_WRF_CHARSTC3
FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocuments` AS PO
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.ekbe` AS ekbe
  ON PO.DocumentNumber_EBELN = ekbe.ebeln
    AND PO.Item_EBELP = ekbe.ebelp
LEFT JOIN tcurx
  ON PO.CurrencyKey_WAERS = tcurx.CURRKEY
LEFT JOIN conv
  ON PO.Client_MANDT = conv.MANDT
    AND PO.CurrencyKey_WAERS = conv.FCURR
    AND CAST(PO.ChangeDate_AEDAT AS DATE) = conv.GDATU
WHERE ekbe.vgabe = '1' #Goods receipt
