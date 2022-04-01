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
  SalesOrders.SoldToParty_KUNNR AS SoldToPartyHeader_KUNNR,
  SalesOrders.SoldToPartyItem_KUNNR,
  SalesOrders.ShipToPartyItem_KUNNR,
  SalesOrders.BillToPartyItem_KUNNR,
  SalesOrders.PayerItem_KUNNR,
  --SalesOrders.SoldToPartyHeader_KUNNR,
  SalesOrders.ShipToPartyHeader_KUNNR,
  SalesOrders.BillToPartyHeader_KUNNR,
  SalesOrders.PayerHeader_KUNNR,
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
  -- Billing.SdDocumentCurrency_WAERK AS BillingDOcumentCurrency_WAERK,
  SalesOrganizationsMD.SalesOrgName_VTEXT AS SalesOrganizationName,
  DistributionChannelMD.DistributionChannelName_VTEXT AS DistributionChannelName,
  RegionMD.CountryName_LANDX AS RegionDescription,
  Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG * Deliveries.NetValueInDocumentCurrency_NETWR AS NetValue,
  SalesOrders.CumulativeOrderQuantity_KWMENG * SalesOrders.Netprice_NETPR AS SalesOrderNetValue,
  SUM(Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG * Deliveries.NetPrice_NETPR) OVER(PARTITION BY Deliveries.DeliveryItem_POSNR, Deliveries.Delivery_VBELN) AS DeliveredNetValue,
  IF(Deliveries. Date__proofOfDelivery___PODAT > SalesOrders.Requesteddeliverydate_VDATU,
    'Delayed',
    'NotDelayed') AS LateDeliveries,
  IF(Deliveries.DeliveryBlock_documentHeader_LIFSK IS NULL
    AND Deliveries.BillingBlockInSdDocument_FAKSK IS NULL,
    'NotBlocked',
    'Blocked' ) AS BlockedSalesOrder,
  /* Count of SalesOrderNumber*/
  COUNT(DISTINCT SalesOrders.SalesDocument_VBELN) OVER(PARTITION BY SalesOrders.Client_MANDT) AS TotalOrders,

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
  IF(SalesOrders.CumulativeOrderQuantity_KWMENG < SalesOrders.ConfirmedOrderQuantity_BMENG,
    'BackOrder',
    'NotBackOrder') AS BackOrder,

  /*Open Orders*/
  IF(Deliveries.ActualQuantityDelivered_InSalesUnits_LFIMG = SalesOrders.CumulativeOrderQuantity_KWMENG
    AND SalesOrders.CumulativeOrderQuantity_KWMENG = Billing.ActualBilledQuantity_FKIMG,
    'NotOpenOrder',
    'OpenOrder') AS OpenOrder,

  /*ReturnOrder
  IF
  ( DocumentCategory_VBTYP='H',
  IF
  ( PrecedingDocCategory_VGTYP='C'AND SalesDocument_VBELN = SalesOrders.Documentnumberofthereferencedocument_VGBEL
  AND Item_POSNR = ReferenceItem_VGPOS,
  'Returned',
  'NotReturned'),
  IF
  ( PrecedingDocCategory_VGTYP='H'AND BillingDocument_VBELN = Billing.DocumentNumberOfTheReferenceDocument_VGBEL
  AND BillingItem_POSNR = ItemNumberOfTheReferenceItem_VGPOS
  AND SalesDocument_AUBEL=SalesDocument_VBELN
  AND SalesDocumentItem_AUPOS=Item_POSNR,
  'Returned',
  'NotReturned') ) ReturnOrder,*/
  IF( SalesOrders.DocumentCategory_VBTYP = 'H',
    'Returned',
    'NotReturned') AS ReturnOrder,
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
  `{{ project_id_src }}.{{ dataset_reporting_tgt }}.Billing` AS Billing
  ON
    SalesOrders.SalesDocument_VBELN = Billing.SalesDocument_AUBEL
    AND SalesOrders.Item_POSNR = Billing.SalesDocumentItem_AUPOS
    AND SalesOrders.Client_MANDT = Billing.Client_MANDT
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_reporting_tgt }}.CustomersMD` AS CustomersMD
  ON
    SalesOrders.SoldtoParty_KUNNR = CustomersMD.CustomerNumber_KUNNR
    AND SalesOrders.Client_MANDT = CustomersMD.Client_MANDT
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_reporting_tgt }}.MaterialsMD` AS MaterialsMD
  ON
    SalesOrders.MaterialNumber_MATNR = MaterialsMD.MaterialNumber_MATNR
    AND SalesOrders.Client_MANDT = MaterialsMD.Client_MANDT
LEFT JOIN
  `{{ project_id_src }}.{{ dataset_reporting_tgt }}.SalesOrganizationsMD` AS SalesOrganizationsMD
  ON
    SalesOrders.Client_MANDT = SalesOrganizationsMD.Client_MANDT
    AND SalesOrders.SalesOrganization_VKORG = SalesOrganizationsMD.SalesOrg_VKORG

LEFT JOIN
  `{{ project_id_src }}.{{ dataset_reporting_tgt }}.DistributionChannelsMD` AS DistributionChannelMD
  ON
    SalesOrders.Client_MANDT = DistributionChannelMD.Client_MANDT
    AND SalesOrders.DistributionChannel_VTWEG = DistributionChannelMD.DistributionChannel_VTWEG

LEFT JOIN
  `{{ project_id_src }}.{{ dataset_reporting_tgt }}.RegionsMD` AS RegionMD
  ON
    SalesOrders.Client_MANDT = RegionMD.Client_MANDT
    AND CustomersMD.CountryKey_LAND1 = RegionMD.CountryKey_LAND1
  --WHERE (DeliveryBlock_documentHeader_LIFSK IS NOT NULL --OR BillingBlockInSdDocument_FAKSK IS NOT NULL)
