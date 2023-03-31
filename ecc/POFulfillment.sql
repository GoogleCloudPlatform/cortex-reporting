(WITH eket AS (
  SELECT Client_MANDT,
    PurchasingDocumentNumber_EBELN,
    ItemNumberOfPurchasingDocument_EBELP, StatisticsRelevantDeliveryDate_SLFDT,
    ItemDeliveryDate_EINDT, sum(ScheduledQuantity_MENGE) AS ScheduledQuantity_MENGE,
    sum(QuantityOfGoodsReceived_WEMNG) AS QuantityOfGoodsReceived_WEMNG
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.POSchedule`
  GROUP BY Client_MANDT, PurchasingDocumentNumber_EBELN,
    ItemNumberOfPurchasingDocument_EBELP,
    StatisticsRelevantDeliveryDate_SLFDT, ItemDeliveryDate_EINDT
)

, ekpo AS (
  SELECT Client_MANDT,
    PurchasingDocumentNumber_EBELN,
    ItemNumberOfPurchasingDocument_EBELP,
    DeletionFlag_LOEKZ,
	Article_MATNR,
	Plant_WERKS,
	StorageLocation_LGORT,
	CompletedFlag_ELIKZ,
	DocCategory_BSTYP,
	PurReqNumber_BANFN,
	PurReqItemNumberBNFPO,
	ReturnsItemRETPO,
	IssuingStorageLocation_RESLO,
	FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.POItem` #-- To be created ?.
)
	
	
SELECT
  PO.Client_MANDT, PO.DocumentNumber_EBELN, PO.Item_EBELP,
  PO.DocumentCategory_BSTYP, PO.DocumentType_BSART,
  PO.VendorAccountNumber_LIFNR, PO.Language_SPRAS,
  PO.TermsPaymentKey_ZTERM, PO.CashDiscountPercentage1_ZBD1P,
  PO.PurchasingOrganization_EKORG, PO.PurchasingGroup_EKGRP,
  PO.CurrencyKey_WAERS, PO.MaterialNumber_MATNR,
  PO.ShortText_TXZ01,
  PO.MaterialGroup_MATKL,
  PO.StorageLocation_LGORT,
  PO.POQuantity_MENGE,
  PO.UoM_MEINS,
  PO.OrderPriceUnit_BPRME,
  PO.NetPrice_NETPR, PO.NetOrderValueinPOCurrency_NETWR,
  PO.GrossordervalueinPOcurrency_BRTWR,
  PO.DeliveryCompletedFlag_ELIKZ,
  PO.NetWeight_NTGEW,
  PO.ReturnsItem_RETPO,
  delivery.ItemDeliveryDate_EINDT,
  Items.DeletionFlag_LOEKZ,
  Items.Article_MATNR,
  Items.Plant_WERKS,
  Items.StorageLocation_LGORT,
  Items.CompletedFlag_ELIKZ,
  Items.DocCategory_BSTYP,
  Items.PurReqNumber_BANFN,
  Items.PurReqItemNumberBNFPO,
  Items.ReturnsItemRETPO,
  Items.IssuingStorageLocation_RESLO,
  if(PO.ReturnsItem_RETPO IS NULL, delivery.ScheduledQuantity_MENGE, delivery.ScheduledQuantity_MENGE * -1 ) AS TotalScheduledQty,
  if(PO.ReturnsItem_RETPO IS NULL, delivery.QuantityOfGoodsReceived_WEMNG, delivery.QuantityOfGoodsReceived_WEMNG * -1 ) AS TotalReceivedQty,
  (if (Items.DeletionFlag_LOEKZ IS NULL AND Items.CompletedFlag_ELIKZ IS NULL (if(PO.ReturnsItem_RETPO IS NULL, delivery.ScheduledQuantity_MENGE, delivery.ScheduledQuantity_MENGE * -1 ) - if(PO.ReturnsItem_RETPO IS NULL, delivery.QuantityOfGoodsReceived_WEMNG, delivery.QuantityOfGoodsReceived_WEMNG * -1 ) )),0) AS PendingQty
FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocuments` AS PO
INNER JOIN eket AS delivery
  ON PO.Client_MANDT = delivery.Client_MANDT
    AND PO.DocumentNumber_EBELN = delivery.PurchasingDocumentNumber_EBELN
    AND PO.Item_EBELP = delivery.ItemNumberOfPurchasingDocument_EBELP
INNER JOIN ekpo AS Items
  ON PO.Client_MANDT = item.Client_MANDT
    AND PO.DocumentNumber_EBELN = item.PurchasingDocumentNumber_EBELN
    AND PO.Item_EBELP = item.ItemNumberOfPurchasingDocument_EBELP
