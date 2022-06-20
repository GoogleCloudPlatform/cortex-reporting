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

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.OrderToCash`
OPTIONS(
description = "Order to Cash Reporting"
)
AS
WITH
OneTouchOrder AS (
  SELECT distinct
 VBAPClient_MANDT,
  VBAPSalesDocument_VBELN,
 VBAPSalesDocument_Item_POSNR,
  VBAPTotalOrder_KWMENG,
  vbrp.fkimg,
   OneTouchOrderCount
 FROM
  (
  SELECT
    vbap.mandt VBAPClient_MANDT,
    vbap.vbeln VBAPSalesDocument_VBELN,
    vbap.posnr VBAPSalesDocument_Item_POSNR,
    vbap.kwmeng VBAPTotalOrder_KWMENG,
    vbap.netwr NetValueOfTheOrderItemInDocumentCurrency_NETWR_vbap,
    vbap.recordstamp RecordTimeStamp_vbap,
    vbep.mandt Client_MANDT_vbep,
    vbep.vbeln SalesDocument_VBELN_vbep,
    vbep.posnr SalesDocumentItem_POSNR_vbep,
    vbep.etenr ScheduleLineNumber_ETENR,
    vbep.bmeng ConfirmedQuantity_BMENG,
    lips.mandt Client_MANDT_lips,
    lips.vbeln Delivery_VBELN_lips,
    lips.posnr DeliveryItem_POSNR_lips,
    lips.erdat CreationDate_ERDAT,
    lips.aedat DateOfLastChange_AEDAT,
    lips.recordstamp RecordTimeStamp_lips,
    COUNT(*) OneTouchOrderCount
  FROM
    `{{ project_id_src }}.{{ dataset_raw_landing }}.vbap` vbap,
    `{{ project_id_src }}.{{ dataset_raw_landing }}.vbep` vbep,
    `{{ project_id_src }}.{{ dataset_raw_landing }}.lips` lips
  WHERE
    vbap.mandt=vbep.mandt
    AND vbap.vbeln = vbep.vbeln
    AND vbap.posnr = vbep.posnr
    AND vbap.mandt=lips.mandt
    AND vbap.vbeln=lips.vgbel
    AND vbap.posnr=lips.vgpos
  GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  HAVING
    COUNT(*)<2 ) OneTouchOrder
 JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.vbrp` AS VBRP
 ON
   OneTouchOrder.VBAPClient_MANDT=vbrp.mandt
  AND OneTouchOrder.VBAPSalesDocument_VBELN = vbrp.aubel
  AND OneTouchOrder.VBAPSalesDocument_Item_POSNR = vbrp.posnr
WHERE
  VBAPTotalOrder_KWMENG=vbrp.fkimg --and OneTouchOrderCount=1
)
SELECT
  SalesOrders.Client_MANDT,
  Deliveries.Delivery_VBELN,
  Deliveries.DeliveryItem_POSNR,
  SalesOrders.SalesDocument_VBELN,
  SalesOrders.Item_POSNR,
  Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG,
  Deliveries.BaseUnitOfMeasure_MEINS as BaseUoM_MEINS,
  Deliveries.NetPrice_NETPR,
  Deliveries.NetValueInDocumentCurrency_NETWR,
  Deliveries.SdDocumentCurrency_WAERK AS DeliveryDocumentCurrency_WAERK,
  Deliveries.DeliveryBlock_DocumentHeader_LIFSK,
  Deliveries.BillingBlockInSdDocument_FAKSK,
  Deliveries.Date__proofOfDelivery___PODAT,
  Deliveries.BillingDateForBillingIndexAndPrintout_FKDAT,
  Deliveries.SalesOrderNumber_VGBEL AS SalesOrderNumber,
  Deliveries.DeliveryDate_LFDAT,
  Deliveries.ActualGoodsMovementDate_WADAT_IST,
  SalesOrders.ExchangeRateType_KURST,
  SalesOrders.Requesteddeliverydate_VDATU,
  SalesOrders.CumulativeOrderQuantity_KWMENG AS SalesOrderQty,
  SalesOrders.BaseUnitofMeasure_MEINS,
  SalesOrders.Netprice_NETPR AS SalesOrderNetPrice,
  SalesOrders.Currency_WAERK AS SalesOrderDocumentCurrency_WAERK,
  SalesOrders.ShippingReceivingPoint_VSTEL AS ShippingLocation,
  salesorders.SoldToParty_KUNNR,
  salesorders.SoldToPartyItem_KUNNR,
  salesorders.SoldToPartyItemName_KUNNR,
  salesorders.ShipToPartyItem_KUNNR,
  salesorders.ShipToPartyItemName_KUNNR,
  salesorders.BillToPartyItem_KUNNR,
  salesorders.BillToPartyItemName_KUNNR,
  salesorders.PayerItem_KUNNR,
  salesorders.PayerItemName_KUNNR,
  salesorders.SoldToPartyHeader_KUNNR,
  salesorders.SoldToPartyHeaderName_KUNNR,
  salesorders.ShipToPartyHeader_KUNNR,
  salesorders.ShipToPartyHeaderName_KUNNR,
  salesorders.BillToPartyHeader_KUNNR,
  salesorders.BillToPartyHeaderName_KUNNR,
  salesorders.PayerHeader_KUNNR,
  salesorders.PayerHeaderName_KUNNR,
  SalesOrders.SalesOrganization_VKORG AS SalesOrganization,
  SalesOrders.DistributionChannel_VTWEG AS DistributionChannel,
  SalesOrders.OverallDeliveryStatus_LFGSK AS DeliveryStatus,
  SalesOrders.ListPrice AS ListPrice,
  SalesOrders.AdjustedPrice AS AdjustedPrice,
  SalesOrders.InterCompanyPrice AS InterCompanyPrice,
  SalesOrders.Discount AS Discount,
  SalesOrders.ConfirmedOrderQuantity_BMENG,
  SalesOrders.BaseUnitofMeasure_MEINS AS SalesUnitMeasure,
  SalesOrders.CreationDate_ERDAT,
  SalesOrders.DocumentCategory_VBTYP,
  SalesOrders.PrecedingDocCategory_VGTYP,
  SalesOrders.Documentnumberofthereferencedocument_VGBEL,
  SalesOrders.ReferenceItem_VGPOS,
  SalesOrders.RejectionReason_ABGRU,
  CustomersMD.CustomerNumber_KUNNR AS CustomerNumber,
  CustomersMD.Name1_NAME1 AS CustomerName1,
  CustomersMD.Name2_NAME2 AS CustomerName2,
  CustomersMD.City_ORT01 AS City,
  CustomersMD.CountryKey_LAND1 AS Country,
  CustomersMD.PostalCode_PSTLZ AS PostalCode,
  CustomersMD.CustomerRegion_REGIO AS CustomerRegion,
  CustomersMD.Address_ADRNR AS CustomerAddress,
  MaterialsMD.MaterialNumber_MATNR AS MaterialNumber,
  MaterialsMD.MaterialType_MTART AS MaterialType,
  MaterialsMD.Division_SPART AS Division,
  MaterialsMD.MaterialCategory_ATTYP AS ProductCategory,
  MaterialsMD.Brand_BRAND_ID AS Brand,
  MaterialsMD.MaterialText_MAKTX AS MaterialDescription,
  Billing.ActualBilledQuantity_FKIMG AS BilledQty,
  Billing.BillingDocument_VBELN,
  Billing.Rebate,
  Billing.TaxAmount_MWSBK,
  Billing.Volume_VOLUM,
  Billing.GrossWeight_BRGEW,
  Billing.BillingDate_FKDAT,
  Billing.BillingItem_POSNR,
  Billing.NetWeight_NTGEW,
  Billing.NetValue_NETWR AS BillingNetValue,
  --Billing.SdDocumentCurrency_WAERK AS BillingDOcumentCurrency_WAERK,
  SalesOrganizationsMD.SalesOrgName_VTEXT AS SalesOrganizationName,
  DistributionChannelMD.DistributionChannelName_VTEXT AS DistributionChannelName,
  CountriesMD.CountryName_LANDX AS RegionDescription,
  OneTouchOrder.OneTouchOrderCount OneTouchOrderCount,
  OneTouchOrder.VBAPSalesDocument_VBELN OneTouchOrders,
  Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG * Deliveries.NetPrice_NETPR DeliveredValue,
  Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG*Deliveries.NetValueInDocumentCurrency_NETWR Value,
  SalesOrders.CumulativeOrderQuantity_KWMENG * SalesOrders.Netprice_NETPR AS SalesOrderNetValue,
  SUM(Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG * Deliveries.NetPrice_NETPR) OVER(PARTITION BY Deliveries.DeliveryItem_POSNR, Deliveries.Delivery_VBELN) AS DeliveredNetValue,
  IF(Deliveries. Date__proofOfDelivery___PODAT > Deliveries.DeliveryDate_LFDAT,
    'Delayed',
    'NotDelayed') AS LateDeliveries,
  IF(Deliveries.DeliveryBlock_documentHeader_LIFSK IS NULL
    AND Deliveries.BillingBlockInSdDocument_FAKSK IS NULL,
    'NotBlocked',
    'Blocked' ) AS BlockedSalesOrder,
  /* Count of SalesOrderNumber*/
  COUNT(DISTINCT SalesOrders.SalesDocument_VBELN) OVER(PARTITION BY SalesOrders.Client_MANDT) AS TotalOrders,
  /* Count of Sales order Item */
count(SalesOrders.Item_POSNR) OVER(PARTITION BY SalesOrders.Client_MANDT) TotalOrderItems,

  /* Count of TotalDeliveryItems*/
  COUNT(Deliveries.DeliveryItem_POSNR) OVER(PARTITION BY SalesOrders.Client_MANDT) AS TotalDeliveries,
  /*SalesOrderQuantityHeaderLevel*/
  SUM(SalesOrders.CumulativeOrderQuantity_KWMENG ) OVER(PARTITION BY SalesOrders.SalesDocument_VBELN, SalesOrders.Item_POSNR) AS SalesOrderQuantity,

  /*SalesOrderValueHeaderLevel*/
  SUM(SalesOrders.Netprice_NETPR * SalesOrders.CumulativeOrderQuantity_KWMENG) OVER(PARTITION BY SalesOrders.SalesDocument_VBELN, SalesOrders.Item_POSNR) AS SalesOrderValue,

  /*Count OF Incoming SalesOrders*/
  IF(SalesOrders.DocumentCategory_VBTYP = 'C',
    SalesOrders.SalesDocument_VBELN,
    NULL) AS IncomingOrderNum,

  /*InFullDelivery*/
  IF(SalesOrders.CumulativeOrderQuantity_KWMENG = Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG,
    'DeliveredInFull',
    'NotDeliverdInFull') AS InFullDelivery,

  /*OTIF*/
  IF(Deliveries. Date__proofOfDelivery___PODAT <= Deliveries.DeliveryDate_LFDAT
    AND SalesOrders.CumulativeOrderQuantity_KWMENG = Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG,
    'OTIF',
    'NotOTIF') AS OTIF,

  /*IF
  (IF
    (Deliveries.Date__proofOfDelivery___PODAT IS NULL,
    CURRENT_DATE(),
    Deliveries.Date__proofOfDelivery___PODAT)>SalesOrders.Requesteddeliverydate_VDATU
    AND CumulativeOrderQuantity_KWMENG=ActualQuantityDelivered_InSalesUnits_LFIMG,
    'OTIF',
    'NotOTIF') OTIF,*/

  /*FillRate*/
  SalesOrders.ConfirmedOrderQuantity_BMENG / SUM(SalesOrders.CumulativeOrderQuantity_KWMENG ) OVER(PARTITION BY SalesOrders.SalesDocument_VBELN, SalesOrders.Item_POSNR) * 100 AS FillRatePercent,

  /* BackOrder*/
  IF(SalesOrders.CumulativeOrderQuantity_KWMENG > SalesOrders.ConfirmedOrderQuantity_BMENG,
    'BackOrder',
    'NotBackOrder') AS BackOrder,

  /*Open Orders*/
  IF(Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG = SalesOrders.CumulativeOrderQuantity_KWMENG
    AND SalesOrders.CumulativeOrderQuantity_KWMENG = Billing.ActualBilledQuantity_FKIMG,
    'NotOpenOrder',
    'OpenOrder') AS OpenOrder,

  /*ReturnOrder*/
  IF
  ( DocumentCategory_VBTYP='H',
  IF
  ( PrecedingDocCategory_VGTYP='C' AND SalesOrders.ReferenceDocument_VGBEL = SalesOrders.Documentnumberofthereferencedocument_VGBEL
  AND Item_POSNR = ReferenceItem_VGPOS,
  'Returned',
  'NotReturned'),
  IF
  ( PrecedingDocCategory_VGTYP='H' AND BillingDocument_VBELN = Billing.DocumentNumberOfTheReferenceDocument_VGBEL
  AND BillingItem_POSNR = ItemNumberOfTheReferenceItem_VGPOS
  AND SalesDocument_AUBEL=SalesDocument_VBELN
  AND SalesDocumentItem_AUPOS=Item_POSNR,
  'Returned',
  'NotReturned') ) ReturnOrder,
  /*IF( SalesOrders.DocumentCategory_VBTYP = 'H',
    'Returned',
    'NotReturned') AS ReturnOrder,*/
  /*IF
  (DeliveryBlock_documentHeader_LIFSK IS NOT NULL
    OR BillingBlockInSdDocument_FAKSK IS NOT NULL,
    ActualQuantityDelivered_InSalesUnits_LFIMG,
    NULL ) Blocked_Quantity,
  IF
  (DeliveryBlock_documentHeader_LIFSK IS NOT NULL
    OR BillingBlockInSdDocument_FAKSK IS NOT NULL,
    ActualQuantityDelivered_InSalesUnits_LFIMG*NetValueInDocumentCurrency_NETWR,
    NULL ) Blocked_Value,*/

  /* CancelledOrders*/
  IF(SalesOrders.RejectionReason_ABGRU IS NOT NULL,
    'Canceled',
    'NotCanceled') AS CanceledOrder,

  /*OrderCycleTimeInDays*/
  IF(Deliveries.ActualGoodsMovementDate_WADAT_IST IS NOT NULL,
    TIMESTAMP_DIFF(CAST(CONCAT(Deliveries.Date__proofOfDelivery___PODAT, " ", Deliveries.ConfirmationTime_POTIM) AS TIMESTAMP),
      CAST(CONCAT(SalesOrders.CreationDate_ERDAT, " ", SalesOrders.CreationTime_ERZET) AS TIMESTAMP), DAY), NULL) AS OrderCycleTimeInDays,

  /*OnTimeDelivery*/
  IF(Deliveries.Date__proofOfDelivery___PODAT <= Deliveries.DeliveryDate_LFDAT,
    'DeliveredOnTime',
    'NotDeliveredOnTime') AS OnTimeDelivery
  /*IF
  (
  IF
    (Deliveries.Date__proofOfDelivery___PODAT IS NULL,
    CURRENT_DATE(),
    Deliveries.Date__proofOfDelivery___PODAT)>SalesOrders.Requesteddeliverydate_VDATU,
    'DeliveredOnTime',
    'NotDeliveredOnTime') OnTimeDelivery,*/

FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesOrders` AS SalesOrders
LEFT JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Deliveries` AS Deliveries
  ON
    SalesOrders.SalesDocument_VBELN = Deliveries.SalesOrderNumber_VGBEL
    AND SalesOrders.Item_POSNR = Deliveries.SalesOrderItem_VGPOS
    AND SalesOrders.Client_MANDT = Deliveries.Client_MANDT
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Billing` AS Billing
  ON
    SalesOrders.SalesDocument_VBELN = Billing.SalesDocument_AUBEL
    AND SalesOrders.Item_POSNR = Billing.SalesDocumentItem_AUPOS
    AND SalesOrders.Client_MANDT = Billing.Client_MANDT
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CustomersMD` AS CustomersMD
  ON
    SalesOrders.SoldtoParty_KUNNR = CustomersMD.CustomerNumber_KUNNR
    AND SalesOrders.Client_MANDT = CustomersMD.Client_MANDT
    AND CustomersMD.LanguageKey_SPRAS='E'

LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialsMD` AS MaterialsMD
  ON
    SalesOrders.MaterialNumber_MATNR = MaterialsMD.MaterialNumber_MATNR
    AND SalesOrders.Client_MANDT = MaterialsMD.Client_MANDT
    AND MaterialsMD.Language_SPRAS = 'E'
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesOrganizationsMD` AS SalesOrganizationsMD
  ON
    SalesOrders.Client_MANDT = SalesOrganizationsMD.Client_MANDT
    AND SalesOrders.SalesOrganization_VKORG = SalesOrganizationsMD.SalesOrg_VKORG
    AND SalesOrganizationsMD.Language_SPRAS='E'

LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.DistributionChannelsMD` AS DistributionChannelMD
  ON
    SalesOrders.Client_MANDT = DistributionChannelMD.Client_MANDT
    AND SalesOrders.DistributionChannel_VTWEG = DistributionChannelMD.DistributionChannel_VTWEG
    and  DistributionChannelMD.Language_SPRAS='E'

LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Countries_T005` AS CountriesMD
  ON
    SalesOrders.Client_MANDT = CountriesMD.Client_MANDT
    AND CustomersMD.CountryKey_LAND1 = CountriesMD.CountryKey_LAND1
    AND CountriesMD.Language_SPRAS = 'E'
	LEFT JOIN
  OneTouchOrder
  ON
  SalesOrders.Client_MANDT = OneTouchOrder.VBAPClient_MANDT
  AND SalesOrders.SalesDocument_VBELN = OneTouchOrder.VBAPSalesDocument_VBELN
  AND SalesOrders.Item_POSNR = OneTouchOrder.VBAPSalesDocument_Item_POSNR
;

