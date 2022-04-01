WITH AGG_KONV AS (
  SELECT
    konv.knumv AS Knumv,
    konv.kposn AS Kposn,
    konv.mandt AS Mandt,
    SUM(IF(Konv.Koaid = 'C' AND konv.kinak IS NULL, konv.kwert, NULL )) AS Rebate
  FROM
    `{{ project_id_src }}.{{ dataset_cdc_processed }}.konv` AS konv
  GROUP BY 1, 2, 3

)

SELECT
  vbrk.MANDT AS Client_MANDT,
  vbrk.FKART AS BillingType_FKART,
  vbrk.FKTYP AS BillingCategory_FKTYP,
  vbrk.VKORG AS SalesOrganization_VKORG,
  vbrk.VTWEG AS DistributionChannel_VTWEG,
  vbrk.SPART AS Division_SPART,
  vbrk.VBTYP AS SDDocumentCategory_VBTYP,
  vbrk.BZIRK AS SalesDistrict_BZIRK,
  vbrk.PLTYP AS PriceListType_PLTYP,
  vbrk.FKSTO AS BillingDocumentIsCancelled_FKSTO,
  /*vbrk.BLART AS DocumentType_BLART,
  vbrk.GBSTK AS OverallProcessingStatus_GBSTK,
  vbrk.BUCHK AS PostingStatusOfBillingDocument_BUCHK,
  vbrk.RELIK AS InvoiceListStatusOfBillingDocument_RELIK,
  vbrk.UVALS AS IncompletionStatus_UVALS,
  vbrk.UVPRS AS PricingIncompletionStatus_UVPRS,
  vbrk.FKSAK AS BillingStatus_FKSAK,
  vbrk.ABSTK AS RejectionStatus_ABSTK,*/
  vbrk.KUNRG AS Payer_KUNRG,
  vbrk.INCO1 AS IncotermsPart1_INCO1,
  vbrk.INCO2 AS IncotermsPart2_INCO2,
  vbrk.LAND1 AS DestinationCountry_LAND1,
  vbrk.REGIO AS Region_REGIO,
  vbrk.COUNC AS CountryCode_COUNC,
  vbrk.CITYC AS CityCode_CITYC,
  vbrk.TAXK1 AS TaxClassification1ForCustomer_TAXK1,
  vbrk.TAXK2 AS TaxClassification2ForCustomer_TAXK2,
  vbrk.TAXK3 AS TaxClassification3ForCustomer_TAXK3,
  vbrk.TAXK4 AS TaxClassification4ForCustomer_TAXK4,
  vbrk.TAXK5 AS TaxClassification5ForCustomer_TAXK5,
  vbrk.LANDTX AS TaxDepartureCountry_LANDTX,
  vbrk.STCEG_H AS OriginOfSalesTaxIDNumber_STCEG_H,
  vbrk.STCEG_L AS CountryOfSalesTaxIDNumber_STCEG_L,
  vbrk.XBLNR AS ReferenceDocumentNumber_XBLNR,
  vbrk.KONDA AS CustomerPriceGroup_KONDA,
  vbrk.RFBSK AS StatusForTransferToAccounting_RFBSK,
  vbrk.FKDAT AS BillingDate_FKDAT,
  vbrk.GJAHR AS FiscalYear_GJAHR,
  vbrk.POPER AS PostingPeriod_POPER,
  vbrk.ERDAT AS RecordCreationDate_ERDAT,
  vbrk.AEDAT AS LastChangeDate_AEDAT,
  vbrk.MWSBK AS TaxAmount_MWSBK,
  vbrk.KDGRP AS CustomerGroup_KDGRP,
  vbrk.ZLSCH AS PaymentMethod_ZLSCH,
  vbrk.BUKRS AS CompanyCode_BUKRS,
  vbrk.MSCHL AS DunningKey_MSCHL,
  vbrk.MANSP AS DunningBlock_MANSP,
  vbrk.KUNAG AS SoldToParty_KUNAG,
  vbrk.FKART_AB AS AccrualBillingType_FKART,
  vbrk.BELNR AS AccountingDocumentNumber_BELNR,
  vbrk.VSBED AS ShippingConditions_VSBED,
  vbrp.GSBER AS BusinessArea_GSBER,
  vbrp.VBELN AS BillingDocument_VBELN,
  vbrp.POSNR AS BillingItem_POSNR,
  vbrp.PSTYV AS SalesDocumentItemCategory_PSTYV,
  vbrp.POSAR AS ItemType_POSAR,
  vbrp.KOSTL AS CostCenter_KOSTL,
  vbrp.VKGRP AS SalesGroup_VKGRP,
  vbrp.VKBUR AS SalesOffice_VKBUR,
  vbrp.PRCTR AS ProfitCenter_PRCTR,
  vbrp.KOKRS AS ControllingArea_KOKRS,
  vbrp.VGTYP AS DocumentCategoryOfPrecedingSDDocument_VGTYP,
  vbrp.MATNR AS MaterialNumber_MATNR,
  vbrp.PMATN AS PricingReferenceMaterial_PMATN,
  vbrp.CHARG AS BatchNumber_CHARG,
  vbrp.MATKL AS MaterialGroup_MATKL,
  vbrp.PRODH AS ProductHierarchy_PRODH,
  vbrp.WERKS AS Plant_WERKS,
  vbrp.KONDM AS MaterialPriceGroup_KONDM,
  vbrp.LGORT AS StorageLocation_LGORT,
  vbrp.EAN11 AS InternationalArticleNumber_EAN11,
  vbrp.MVGR1 AS MaterialGroup1_MVGR1,
  vbrp.MVGR2 AS MaterialGroup2_MVGR2,
  vbrp.MVGR3 AS MaterialGroup3_MVGR3,
  vbrp.MVGR4 AS MaterialGroup4_MVGR4,
  vbrp.MVGR5 AS MaterialGroup5_MVGR5,
  vbrp.SERNR AS BOMExplosionNumber_SERNR,
  vbrp.KVGR1 AS CustomerGroup1_KVGR1,
  vbrp.KVGR2 AS CustomerGroup2_KVGR2,
  vbrp.KVGR3 AS CustomerGroup3_KVGR3,
  vbrp.KVGR4 AS CustomerGroup4_KVGR4,
  vbrp.KVGR5 AS CustomerGroup5_KVGR5,
  vbrp.TXJCD AS TaxJurisdiction_TXJCD,
  vbrp.VSTEL AS ShippingPointReceivingPoint_VSTEL,
  vbrp.VGBEL AS DocumentNumberOfTheReferenceDocument_VGBEL,
  vbrp.VGPOS AS ItemNumberOfTheReferenceItem_VGPOS,
  vbrp.AUBEL AS SalesDocument_AUBEL,
  vbrp.AUPOS AS SalesDocumentItem_AUPOS,
  vbrp.FKIMG AS ActualBilledQuantity_FKIMG,
  vbrp.NETWR AS NetValue_NETWR,
  vbrp.VOLUM AS Volume_VOLUM,
  vbrp.BRGEW AS GrossWeight_BRGEW,
  vbrp.NTGEW AS NetWeight_NTGEW,
  AGG_KONV.Rebate AS Rebate,
  EXTRACT(YEAR FROM vbrk.FKDAT) AS YearOfBillingDate_FKDAT,
  EXTRACT(MONTH FROM vbrk.FKDAT) AS MonthOfBillingDate_FKDAT,
  EXTRACT(WEEK FROM vbrk.FKDAT) AS WeekOfBillingDate_FKDAT,
  EXTRACT(DAY FROM vbrk.FKDAT) AS DayOfBillingDate_FKDAT,
  LPAD(CAST(AGG_KONV.Knumv AS STRING), 10, '0') AS NumberOfTheDocumentCondition_KNUMV,
  LPAD(CAST(AGG_KONV.Kposn AS STRING), 6, '0') AS ConditionItemNumber_KPOSN,
  COUNT(vbrk.Vbeln) OVER(PARTITION BY EXTRACT(YEAR FROM vbrk.Fkdat)) AS YearOrderCount,
  COUNT(
    vbrk.Vbeln
  ) OVER(
    PARTITION BY EXTRACT(YEAR FROM vbrk.Fkdat), EXTRACT(MONTH FROM vbrk.Fkdat)
  ) AS MonthOrderCount,
  COUNT(
    vbrk.Vbeln
  ) OVER(
    PARTITION BY
      EXTRACT(YEAR FROM vbrk.Fkdat), EXTRACT(MONTH FROM vbrk.Fkdat), EXTRACT(WEEK FROM vbrk.Fkdat)
  ) AS WeekOrderCount
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.vbrk` AS vbrk
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.vbrp` AS vbrp
  ON
    vbrk.VBELN = vbrp.VBELN
    AND vbrk.Mandt = vbrp.Mandt
INNER JOIN AGG_KONV
  ON CAST(AGG_KONV.Knumv AS string) = vbrk.Knumv
    AND CAST(AGG_KONV.Kposn AS string) = vbrp.Posnr
    AND AGG_KONV.Mandt = vbrp.MANDT
