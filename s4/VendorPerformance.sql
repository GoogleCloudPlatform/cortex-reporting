WITH
  -- Purchase Order Item level details
  PurchaseOrderScheduleLine AS (
    SELECT
      PurchaseOrders.Client_MANDT,
      PurchaseOrders.DocumentNumber_EBELN,
      PurchaseOrders.Item_EBELP,
      PurchaseOrders.DeliveryCompletedFlag_ELIKZ,
      PurchaseOrders.PurchasingDocumentDate_BEDAT,
      PurchaseOrders.NetOrderValueinPOCurrency_NETWR,
      PurchaseOrders.CurrencyKey_WAERS,
      PurchaseOrders.POQuantity_MENGE,
      PurchaseOrders.UoM_MEINS,
      PurchaseOrders.NetPrice_NETPR,
      --## CORTEX-CUSTOMER If you prefer to use amount in Target Currency, uncomment below and
      --## uncomment currency_conversion in PurchaseDocuments
      -- PurchaseOrders.NetPriceInTargetCurrency_NETPR,
      -- PurchaseOrders.NetOrderValueInTargetCurrency_NETWR,
      PurchaseOrders.CreatedOn_AEDAT,
      PurchaseOrders.Status_STATU,
      PurchaseOrders.MaterialNumber_MATNR,
      PurchaseOrders.MaterialType_MTART,
      PurchaseOrders.MaterialGroup_MATKL,
      PurchaseOrders.PurchasingOrganization_EKORG,
      PurchaseOrders.PurchasingGroup_EKGRP,
      PurchaseOrders.VendorAccountNumber_LIFNR,
      PurchaseOrders.Company_BUKRS,
      PurchaseOrders.Plant_WERKS,
      PurchaseOrders.UnderdeliveryToleranceLimit_UNTTO,
      PurchaseOrders.OverdeliveryToleranceLimit_UEBTO,
      POScheduleLine.ItemDeliveryDate_EINDT,
      POScheduleLine.OrderDateOfScheduleLine_BEDAT,
      PurchaseOrders.YearOfPurchasingDocumentDate_BEDAT,
      PurchaseOrders.MonthOfPurchasingDocumentDate_BEDAT,
      PurchaseOrders.WeekOfPurchasingDocumentDate_BEDAT,
      COALESCE(
        (PurchaseOrders.UnderdeliveryToleranceLimit_UNTTO * PurchaseOrders.POQuantity_MENGE) / 100,
        0) AS UnderdeliveryToleranceLimit,
      COALESCE(
        (PurchaseOrders.OverdeliveryToleranceLimit_UEBTO * PurchaseOrders.POQuantity_MENGE) / 100,
        0) AS OverdeliveryToleranceLimit
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchaseDocuments` AS PurchaseOrders
    -- PO Schedule Lines details for PO Item
    LEFT JOIN
      (
        SELECT
          Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP,
          MAX(ItemDeliveryDate_EINDT) AS ItemDeliveryDate_EINDT,
          MAX(OrderDateOfScheduleLine_BEDAT) AS OrderDateOfScheduleLine_BEDAT
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.POSchedule`
        GROUP BY Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP
      ) AS POScheduleLine
      ON
        PurchaseOrders.Client_MANDT = POScheduleLine.Client_MANDT
        AND PurchaseOrders.DocumentNumber_EBELN = POScheduleLine.PurchasingDocumentNumber_EBELN
        AND PurchaseOrders.Item_EBELP = POScheduleLine.ItemNumberOfPurchasingDocument_EBELP
    --## DocumentType_BSART='NB' or 'ENB'->Standrad PO
    --## ItemCategoryinPurchasingDocument_PSTYP ='2'-> Consignment PO
    WHERE PurchaseOrders.DocumentType_BSART IN('NB', 'ENB')
      AND PurchaseOrders.ItemCategoryinPurchasingDocument_PSTYP != '2'
  ),

  -- Getting item historical data.
  -- This join results in mutiple rows for the same item.
  -- This will be aggreagated and brought back at Item level in the next step.
  PurchaseOrdersGoodsReceipt AS (
    SELECT
      PurchaseOrderScheduleLine.Client_MANDT,
      PurchaseOrderScheduleLine.DocumentNumber_EBELN,
      PurchaseOrderScheduleLine.Item_EBELP,
      PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ,
      PurchaseOrderScheduleLine.PurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.NetOrderValueinPOCurrency_NETWR,
      PurchaseOrderScheduleLine.CurrencyKey_WAERS,
      PurchaseOrderScheduleLine.ItemDeliveryDate_EINDT,
      PurchaseOrderScheduleLine.OrderDateOfScheduleLine_BEDAT,
      PurchaseOrderScheduleLine.POQuantity_MENGE,
      PurchaseOrderScheduleLine.UoM_MEINS,
      PurchaseOrderScheduleLine.NetPrice_NETPR,
      PurchaseOrderScheduleLine.CreatedOn_AEDAT,
      PurchaseOrderScheduleLine.Status_STATU,
      PurchaseOrderScheduleLine.MaterialNumber_MATNR,
      PurchaseOrderScheduleLine.MaterialType_MTART,
      PurchaseOrderScheduleLine.MaterialGroup_MATKL,
      PurchaseOrderScheduleLine.PurchasingOrganization_EKORG,
      PurchaseOrderScheduleLine.PurchasingGroup_EKGRP,
      PurchaseOrderScheduleLine.Company_BUKRS,
      PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO,
      PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO,
      PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit,
      PurchaseOrderScheduleLine.OverdeliveryToleranceLimit,
      PurchaseOrderScheduleLine.VendorAccountNumber_LIFNR,
      PurchaseOrderScheduleLine.Plant_WERKS,
      PurchaseOrderScheduleLine.YearOfPurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.MonthOfPurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.WeekOfPurchasingDocumentDate_BEDAT,
      POOrderHistory.AmountInLocalCurrency_DMBTR,
      --## CORTEX-CUSTOMER If you prefer to use amount in Target Currency, uncomment below and
      --## uncomment currency_conversion in POOrderHistory
      -- POOrderHistory.AmountInTargetCurrency_DMBTR,
      POOrderHistory.CurrencyKey_WAERS AS POOrderHistoryCurrencyKey_WAERS,
      --Actual Reciept Date
      IF(
        POOrderHistory.MovementType__inventoryManagement___BWART = '101',
        POOrderHistory.PostingDateInTheDocument_BUDAT,
        NULL) AS PostingDateInTheDocument_BUDAT,

      --DeliveryStatus
      -- TRUE stands for Delivered Orders and FALSE stands for NotDelivered Orders
      IF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ IS NULL,
        FALSE,
        TRUE
      ) AS DeliveredOrNotDelivered,

      --Vendor Cycle Time in Days
      IF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        COALESCE(
          DATE_DIFF(
            IF(
              POOrderHistory.MovementType__inventoryManagement___BWART = '101',
              MAX(POOrderHistory.PostingDateInTheDocument_BUDAT) OVER (
                PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                  PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                  PurchaseOrderScheduleLine.Item_EBELP),
              NULL),
            PurchaseOrderScheduleLine.PurchasingDocumentDate_BEDAT,
            DAY),
          0),
        NULL) AS VendorCycleTimeInDays,

      --Vendor Quality (Rejection)
      -- FALSE stands for Rejected Orders and TRUE stands for NotRejected Orders
      IF(
        POOrderHistory.MovementType__inventoryManagement___BWART IN ('122', '161'),
        FALSE,
        TRUE) AS VendorQuality,
      --Rejected Quantity
      IF(
        POOrderHistory.MovementType__inventoryManagement___BWART IN ('122', '161'),
        POOrderHistory.Quantity_MENGE,
        0) AS RejectedQuantity,

      --Vendor On Time Delivery
      --TRUE stands for NotDelayed Orders and False for Delayed Orders
      IF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IF(
          IF(
            POOrderHistory.MovementType__inventoryManagement___BWART = '101',
            POOrderHistory.PostingDateInTheDocument_BUDAT,
            NULL) <= PurchaseOrderScheduleLine.ItemDeliveryDate_EINDT,
          TRUE,
          FALSE),
        NULL) AS VendorOnTimeDelivery,

      --Vendor InFull Delivery
      -- TRUE stands for DeliveredInFull Orders and FALSE stands for NotDeliveredInFull Orders
      IF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IF(
          PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO IS NULL AND PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO IS NULL,
          IF(
            SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP) >= PurchaseOrderScheduleLine.POQuantity_MENGE,
            TRUE,
            FALSE),
          IF(
            SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP) >= PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit,
            TRUE,
            FALSE)
          OR IF(
            SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP) <= PurchaseOrderScheduleLine.POQuantity_MENGE + PurchaseOrderScheduleLine.OverdeliveryToleranceLimit,
            TRUE,
            FALSE)
        ),
        NULL) AS VendorInFullDelivery,

      --Vendor Invoice Accuracy
      -- TRUE stands for Accurate Invoices and FALSE stands for Inaccurate Invoices
      IF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IF(
          PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO IS NULL AND PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO IS NULL,
          IF(
            PurchaseOrderScheduleLine.POQuantity_MENGE = SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP),
            TRUE,
            FALSE),
          IF(
            SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP)
            BETWEEN PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit
            AND PurchaseOrderScheduleLine.POQuantity_MENGE + purchaseOrderScheduleLine.OverdeliveryToleranceLimit,
            TRUE,
            FALSE)
          OR IF(
            SUM(
              IF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP)
            BETWEEN PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit
            AND PurchaseOrderScheduleLine.POQuantity_MENGE + PurchaseOrderScheduleLine.OverdeliveryToleranceLimit,
            TRUE,
            FALSE)
        ),
        NULL) AS GoodsReceiptAccuracy,

      --Goods Receipt Quantity
      IF(
        POOrderHistory.MovementType__inventoryManagement___BWART = '101',
        POOrderHistory.Quantity_MENGE,
        (POOrderHistory.Quantity_MENGE * -1)
      ) AS GoodsReceiptQuantity,

      --Vendor Spend Analysis In Source Currency
      --Goods Receipt Amount In Source Currency
      IF(POOrderHistory.MovementType__inventoryManagement___BWART = '101',
        POOrderHistory.AmountInLocalCurrency_DMBTR,
        (POOrderHistory.AmountInLocalCurrency_DMBTR * -1)
      ) AS GoodsReceiptAmountInSourceCurrency

    --## CORTEX-CUSTOMER If you prefer to use amount in Target Currency, uncomment below and
    --## uncomment currency_conversion in POOrderHistory
    --Vendor Spend Analysis In Target Currency
    --Goods Receipt Amount In Target Currency
    -- IF(POOrderHistory.MovementType__inventoryManagement___BWART = '101',
    --   POOrderHistory.AmountInTargetCurrency_DMBTR,
    --   (POOrderHistory.AmountInTargetCurrency_DMBTR * -1)
    -- ) AS GoodsReceiptAmountInTargetCurrency
    FROM
      PurchaseOrderScheduleLine
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.POOrderHistory` AS POOrderHistory
      ON
        PurchaseOrderScheduleLine.Client_MANDT = POOrderHistory.Client_MANDT
        AND PurchaseOrderScheduleLine.DocumentNumber_EBELN = POOrderHistory.PurchasingDocumentNumber_EBELN
        AND PurchaseOrderScheduleLine.Item_EBELP = POOrderHistory.ItemNumberOfPurchasingDocument_EBELP
        --## TransactioneventType_VGABE='1' -> Goods Receipt
        AND POOrderHistory.TransactioneventType_VGABE = '1'
        --## MovementType__inventoryManagement___BWART='101' -> Goods Receipt
        --## MovementType__inventoryManagement___BWART='102' -> Returns
        --## MovementType__inventoryManagement___BWART='122' or '161' -> Rejections
        AND POOrderHistory.MovementType__inventoryManagement___BWART IN ('101', '102', '161', '122')
  ),

  PurchaseDocuments AS (
    SELECT
      PurchaseOrdersGoodsReceipt.Client_MANDT,
      PurchaseOrdersGoodsReceipt.DocumentNumber_EBELN,
      PurchaseOrdersGoodsReceipt.Item_EBELP,
      MAX(PurchaseOrdersGoodsReceipt.PurchasingDocumentDate_BEDAT) AS PurchasingDocumentDate_BEDAT,
      AVG(PurchaseOrdersGoodsReceipt.NetOrderValueinPOCurrency_NETWR) AS NetOrderValueinPOCurrency_NETWR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.CurrencyKey_WAERS) AS CurrencyKey_WAERS,
      MAX(PurchaseOrdersGoodsReceipt.ItemDeliveryDate_EINDT) AS ItemDeliveryDate_EINDT,
      MAX(PurchaseOrdersGoodsReceipt.OrderDateOfScheduleLine_BEDAT) AS OrderDateOfScheduleLine_BEDAT,
      MAX(PurchaseOrdersGoodsReceipt.PostingDateInTheDocument_BUDAT) AS PostingDateInTheDocument_BUDAT,
      SUM(PurchaseOrdersGoodsReceipt.AmountInLocalCurrency_DMBTR) AS AmountInLocalCurrency_DMBTR,
      --## CORTEX-CUSTOMER If you prefer to use amount in Target Currency, uncomment below and
      --## uncomment currency_conversion in POOrderHistory
      -- SUM(PurchaseOrdersGoodsReceipt.AmountInTargetCurrency_DMBTR) AS AmountInTargetCurrency_DMBTR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.POOrderHistoryCurrencyKey_WAERS)AS POOrderHistoryCurrencyKey_WAERS,
      AVG(PurchaseOrdersGoodsReceipt.POQuantity_MENGE) AS POQuantity_MENGE,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.UoM_MEINS) AS UoM_MEINS,
      AVG(PurchaseOrdersGoodsReceipt.NetPrice_NETPR) AS NetPrice_NETPR,
      MAX(PurchaseOrdersGoodsReceipt.CreatedOn_AEDAT) AS CreatedOn_AEDAT,
      --## CORTEX-CUSTOMER If you prefer to use amount in Target Currency, uncomment below and
      --## uncomment currency_conversion in PurchaseDocuments
      -- AVG(PurchaseOrdersGoodsReceipt.NetPriceInTargetCurrency_NETPR) AS NetPriceInTargetCurrency_NETPR,
      -- AVG(PurchaseOrdersGoodsReceipt.NetOrderValueInTargetCurrency_NETWR) AS NetOrderValueInTargetCurrency_NETWR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.Status_STATU) AS Status_STATU,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.MaterialNumber_MATNR) AS MaterialNumber_MATNR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.MaterialType_MTART) AS MaterialType_MTART,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.MaterialGroup_MATKL) AS MaterialGroup_MATKL,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.PurchasingOrganization_EKORG) AS PurchasingOrganization_EKORG,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.PurchasingGroup_EKGRP) AS PurchasingGroup_EKGRP,
      ANY_VALUE(PurchaseOrdersGoodsReceipt. VendorAccountNumber_LIFNR) AS VendorAccountNumber_LIFNR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.Company_BUKRS) AS Company_BUKRS,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.Plant_WERKS) AS Plant_WERKS,
      LOGICAL_AND(PurchaseOrdersGoodsReceipt.DeliveredOrNotDelivered) AS DeliveredOrNotDelivered,
      MAX(PurchaseOrdersGoodsReceipt.VendorCycleTimeInDays) AS VendorCycleTimeInDays,
      LOGICAL_AND(PurchaseOrdersGoodsReceipt.VendorQuality) AS VendorQuality,
      SUM(PurchaseOrdersGoodsReceipt.RejectedQuantity) AS RejectedQuantity,
      LOGICAL_AND(PurchaseOrdersGoodsReceipt.VendorOnTimeDelivery) AS VendorOnTimeDelivery,
      LOGICAL_AND(PurchaseOrdersGoodsReceipt.VendorInFullDelivery) AS VendorInFullDelivery,
      LOGICAL_AND(PurchaseOrdersGoodsReceipt.GoodsReceiptAccuracy) AS GoodsReceiptAccuracy,
      SUM(PurchaseOrdersGoodsReceipt.GoodsReceiptQuantity) AS GoodsReceiptQuantity,
      SUM(PurchaseOrdersGoodsReceipt.GoodsReceiptAmountInSourceCurrency) AS GoodsReceiptAmountInSourceCurrency,
      --## CORTEX-CUSTOMER If you prefer to use amount in Target Currency, uncomment below and
      --## uncomment currency_conversion in POOrderHistory
      -- SUM(PurchaseOrdersGoodsReceipt.GoodsReceiptAmountInTargetCurrency) AS GoodsReceiptAmountInTargetCurrency,
      MAX(PurchaseOrdersGoodsReceipt.YearOfPurchasingDocumentDate_BEDAT) AS YearOfPurchasingDocumentDate_BEDAT,
      MAX(PurchaseOrdersGoodsReceipt.MonthOfPurchasingDocumentDate_BEDAT) AS MonthOfPurchasingDocumentDate_BEDAT,
      MAX(PurchaseOrdersGoodsReceipt.WeekOfPurchasingDocumentDate_BEDAT) AS WeekOfPurchasingDocumentDate_BEDAT
    FROM PurchaseOrdersGoodsReceipt
    GROUP BY
      PurchaseOrdersGoodsReceipt.Client_MANDT,
      PurchaseOrdersGoodsReceipt.DocumentNumber_EBELN,
      PurchaseOrdersGoodsReceipt.Item_EBELP
  )

SELECT
  PurchaseDocuments.Client_MANDT,
  PurchaseDocuments.DocumentNumber_EBELN,
  PurchaseDocuments.Item_EBELP,
  MAX(PurchaseDocuments.PurchasingDocumentDate_BEDAT) AS PurchasingDocumentDate_BEDAT,
  AVG(PurchaseDocuments.NetOrderValueinPOCurrency_NETWR) AS NetOrderValueinPOCurrency_NETWR,
  ANY_VALUE(PurchaseDocuments.CurrencyKey_WAERS) AS CurrencyKey_WAERS,
  MAX(PurchaseDocuments.ItemDeliveryDate_EINDT) AS ItemDeliveryDate_EINDT,
  MAX(PurchaseDocuments.OrderDateOfScheduleLine_BEDAT) AS OrderDateOfScheduleLine_BEDAT,
  MAX(PurchaseDocuments.PostingDateInTheDocument_BUDAT) AS PostingDateInTheDocument_BUDAT,
  SUM(PurchaseDocuments.AmountInLocalCurrency_DMBTR) AS AmountInLocalCurrency_DMBTR,
  --## CORTEX-CUSTOMER If you prefer to use amount in Target Currency, uncomment below and
  --## uncomment currency_conversion in POOrderHistory
  -- SUM(PurchaseDocuments.AmountInTargetCurrency_DMBTR) AS AmountInTargetCurrency_DMBTR,
  ANY_VALUE(PurchaseDocuments.POOrderHistoryCurrencyKey_WAERS)AS POOrderHistoryCurrencyKey_WAERS,
  AVG(PurchaseDocuments.POQuantity_MENGE) AS POQuantity_MENGE,
  ANY_VALUE(PurchaseDocuments.UoM_MEINS) AS UoM_MEINS,
  AVG(PurchaseDocuments.NetPrice_NETPR) AS NetPrice_NETPR,
  --## CORTEX-CUSTOMER If you prefer to use amount in Target Currency, uncomment below and
  --## uncomment currency_conversion in PurchaseDocuments
  -- AVG(PurchaseDocuments.NetPriceInTargetCurrency_NETPR) AS NetPriceInTargetCurrency_NETPR,
  -- AVG(PurchaseDocuments.NetOrderValueInTargetCurrency_NETWR) AS NetOrderValueInTargetCurrency_NETWR,
  MAX(PurchaseDocuments.CreatedOn_AEDAT) AS CreatedOn_AEDAT,
  ANY_VALUE(PurchaseDocuments.Status_STATU) AS Status_STATU,
  ANY_VALUE(PurchaseDocuments.MaterialNumber_MATNR) AS MaterialNumber_MATNR,
  ANY_VALUE(PurchaseDocuments.MaterialType_MTART) AS MaterialType_MTART,
  ANY_VALUE(PurchaseDocuments.MaterialGroup_MATKL) AS MaterialGroup_MATKL,
  ANY_VALUE(PurchaseDocuments.PurchasingOrganization_EKORG) AS PurchasingOrganization_EKORG,
  ANY_VALUE(PurchaseDocuments.PurchasingGroup_EKGRP) AS PurchasingGroup_EKGRP,
  ANY_VALUE(PurchaseDocuments.VendorAccountNumber_LIFNR) AS VendorAccountNumber_LIFNR,
  ANY_VALUE(PurchaseDocuments.Company_BUKRS) AS Company_BUKRS,
  ANY_VALUE(PurchaseDocuments.Plant_WERKS) AS Plant_WERKS,
  MAX(PurchaseDocuments.YearOfPurchasingDocumentDate_BEDAT) AS YearOfPurchasingDocumentDate_BEDAT,
  MAX(PurchaseDocuments.MonthOfPurchasingDocumentDate_BEDAT) AS MonthOfPurchasingDocumentDate_BEDAT,
  MAX(PurchaseDocuments.WeekOfPurchasingDocumentDate_BEDAT) AS WeekOfPurchasingDocumentDate_BEDAT,
  --DeliveryStatus
  IF(
    LOGICAL_AND(PurchaseDocuments.DeliveredOrNotDelivered), 'Delivered', 'NotDelivered'
  ) AS DeliveredOrNotDelivered,
  --VendorCycleTime In Days
  MAX(PurchaseDocuments.VendorCycleTimeInDays) AS VendorCycleTimeInDays,
  --Vendor Quality (Rejection)
  IF(
    LOGICAL_AND(PurchaseDocuments.VendorQuality), 'NotRejected', 'Rejected'
  ) AS VendorQuality,
  --Rejected Quantity
  AVG(PurchaseDocuments.RejectedQuantity) AS RejectedQuantity,
  --Vendor On Time Delivery
  IF(
    LOGICAL_AND(PurchaseDocuments.VendorOnTimeDelivery) IS NULL,
    'NotApplicable',
    IF( LOGICAL_AND(PurchaseDocuments.VendorOnTimeDelivery),
      'NotDelayed',
      'Delayed')
  ) AS VendorOnTimeDelivery,
  --Vendor InFull Delivery
  IF(
    LOGICAL_AND(PurchaseDocuments.VendorInFullDelivery) IS NULL,
    'NotApplicable',
    IF(LOGICAL_AND(PurchaseDocuments.VendorInFullDelivery),
      'DeliveredInFull',
      'NotDeliveredInFull')
  ) AS VendorInFullDelivery,
  --Vendor On Time In Full Delivery
  IF(
    LOGICAL_AND(PurchaseDocuments.VendorInFullDelivery) IS NULL OR LOGICAL_AND(PurchaseDocuments.VendorOnTimeDelivery) IS NULL,
    'NotApplicable',
    IF(
      LOGICAL_AND(PurchaseDocuments.VendorInFullDelivery) AND LOGICAL_AND(PurchaseDocuments.VendorOnTimeDelivery),
      'OTIF',
      'NotOTIF')
  )AS VendorOnTimeInFullDelivery,
  --Vendor Invoice Accuracy
  IF(
    LOGICAL_AND(PurchaseDocuments.GoodsReceiptAccuracy) IS NULL OR SUM(PurchaseOrdersInvoiceReceipt.Quantity_MENGE) IS NULL,
    'NotApplicable',
    IF(
      LOGICAL_AND(PurchaseDocuments.GoodsReceiptAccuracy) AND AVG(PurchaseDocuments.POQuantity_MENGE) = SUM(PurchaseOrdersInvoiceReceipt.Quantity_MENGE),
      'AccurateInvoice',
      'InaccurateInvoice')
  ) AS VendorInvoiceAccuracy,
  --Goods Receipt Quantity
  AVG(PurchaseDocuments.GoodsReceiptQuantity) AS GoodsReceiptQuantity,
  --Vendor Spend Analysis (Goods Receipt Amount in Source Currency)
  AVG(PurchaseDocuments.GoodsReceiptAmountInSourceCurrency) AS GoodsReceiptAmountInSourceCurrency,
  --## CORTEX-CUSTOMER If you prefer to use amount in Target Currency, uncomment below and
  --## uncomment currency_conversion in POOrderHistory
  -- AVG(PurchaseOrdersGoodsReceipt.GoodsReceiptAmountInTargetCurrency) AS GoodsReceiptAmountInTargetCurrency,
  --Past Due and Open
  IF(
    LOGICAL_AND(PurchaseDocuments.DeliveredOrNotDelivered),
    'NotApplicable',
    IF(
      CURRENT_DATE() > MAX(PurchaseDocuments.ItemDeliveryDate_EINDT),
      'PastDue',
      'Open')
  ) AS PastDueOrOpenItems,
  --Invoice Quantity
  SUM(PurchaseOrdersInvoiceReceipt.Quantity_MENGE) AS InvoiceQuantity,
  --Vendor Spend Analysis (Invoice Amount in Source Currency)
  SUM(PurchaseOrdersInvoiceReceipt.AmountInLocalCurrency_DMBTR) AS InvoiceAmountInSourceCurrency,
  --## CORTEX-CUSTOMER If you prefer to use amount in Target Currency, uncomment below and
  --## uncomment currency_conversion in POOrderHistory
  -- SUM(PurchaseOrdersInvoiceReceipt.AmountInTargetCurrency_DMBTR) AS InvoiceAmountInTargetCurrency,
  --Invoice Date
  MAX(PurchaseOrdersInvoiceReceipt.PostingDateInTheDocument_BUDAT) AS InvoiceDate,
  MAX(PurchaseOrdersInvoiceReceipt.YearOfPostingDateInTheDocument_BUDAT) AS YearOfInvoiceDate,
  MAX(PurchaseOrdersInvoiceReceipt.MonthOfPostingDateInTheDocument_BUDAT) AS MonthOfInvoiceDate,
  MAX(PurchaseOrdersInvoiceReceipt.WeekOfPostingDateInTheDocument_BUDAT) AS WeekOfInvoiceDate,
  ---Invoice Count
  COUNT(PurchaseOrdersInvoiceReceipt.PurchasingDocumentNumber_EBELN) AS InvoiceCount,
  --The following text fields are language independent.
  ANY_VALUE(PurchasingOrganizations.PurchasingOrganizationText_EKOTX) AS PurchasingOrganizationText_EKOTX,
  ANY_VALUE(PurchasingGroups.PurchasingGroupText_EKNAM) AS PurchasingGroupText_EKNAM,
  ANY_VALUE(Vendors.CountryKey_LAND1) AS CountryKey_LAND1,
  ANY_VALUE(Vendors.NAME1) AS NAME1,
  ANY_VALUE(Companies.CompanyText_BUTXT) AS CompanyText_BUTXT,
  ANY_VALUE(Companies.FiscalyearVariant_PERIV) AS FiscalyearVariant_PERIV
FROM PurchaseDocuments
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.POOrderHistory` AS PurchaseOrdersInvoiceReceipt
  ON
    PurchaseDocuments.Client_MANDT = PurchaseOrdersInvoiceReceipt.Client_MANDT
    AND PurchaseDocuments.DocumentNumber_EBELN = PurchaseOrdersInvoiceReceipt.PurchasingDocumentNumber_EBELN
    AND PurchaseDocuments.Item_EBELP = PurchaseOrdersInvoiceReceipt.ItemNumberOfPurchasingDocument_EBELP
    --## TransactioneventType_VGABE='2' -> Invoice Receipt
    AND PurchaseOrdersInvoiceReceipt.TransactioneventType_VGABE = '2'
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchasingOrganizationsMD` AS PurchasingOrganizations
  ON
    PurchaseDocuments.Client_MANDT = PurchasingOrganizations.Client_MANDT
    AND PurchaseDocuments.PurchasingOrganization_EKORG = PurchasingOrganizations.PurchasingOrganization_EKORG
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.PurchasingGroupsMD` AS PurchasingGroups
  ON
    PurchaseDocuments.Client_MANDT = PurchasingGroups.Client_MANDT
    AND PurchaseDocuments.PurchasingGroup_EKGRP = PurchasingGroups.PurchasingGroup_EKGRP
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.VendorsMD` AS Vendors
  ON
    PurchaseDocuments.Client_MANDT = Vendors.Client_MANDT
    AND PurchaseDocuments.VendorAccountNumber_LIFNR = Vendors.AccountNumberOfVendorOrCreditor_LIFNR
LEFT JOIN
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CompaniesMD` AS Companies
  ON
    PurchaseDocuments.Client_MANDT = Companies.Client_MANDT
    AND PurchaseDocuments.Company_BUKRS = Companies.CompanyCode_BUKRS
GROUP BY
  PurchaseDocuments.Client_MANDT,
  PurchaseDocuments.DocumentNumber_EBELN,
  PurchaseDocuments.Item_EBELP
