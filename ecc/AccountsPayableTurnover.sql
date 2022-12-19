WITH AccountsPayable AS (
  SELECT
    AccountsPayable.Client_MANDT,
    AccountsPayable.CompanyCode_BUKRS,
    AccountsPayable.CompanyText_BUTXT,
    AccountsPayable.AccountNumberOfVendorOrCreditor_LIFNR,
    AccountsPayable.NAME1,
    AccountsPayable.AccountingDocumentNumber_BELNR,
    AccountsPayable.NumberOfLineItemWithinAccountingDocument_BUZEI,
    AccountsPayable.PostingDateInTheDocument_BUDAT,
    AccountsPayable.AccountingDocumenttype_BLART,
    AccountsPayable.AmountInLocalCurrency_DMBTR,
    AccountsPayable.CurrencyKey_WAERS,
    AccountsPayable.DocFiscPeriod,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Movement Types.
        -- Value '101' represents 'GR Goods Receipt' and '501' represents 'Receipt w/o PO'
        AccountsPayable.AccountType_KOART = 'M' AND AccountsPayable.MovementType__inventoryManagement___BWART IN ('101', '501'),
        AccountsPayable.POOrderHistory_AmountInLocalCurrency_DMBTR,
        IF(
          ## CORTEX-CUSTOMER: Please add relevant Movement Types.
          -- Value '102' represents 'GR for PO reversal (full or any one of the line item)'
          -- Value '502' represents 'Return Receipt w/o PO' (Receipt made against movement type 501 document is cancelled.)
          AccountsPayable.AccountType_KOART = 'M' AND AccountsPayable.MovementType__inventoryManagement___BWART IN ('102', '502'),
          AccountsPayable.POOrderHistory_AmountInLocalCurrency_DMBTR * -1, 0
        )
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS, AccountsPayable.DocFiscPeriod
    ) AS TotalPurchases,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountsPayable.Accounttype_KOART = 'K'
        ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'RE' represents 'Invoice - Gross'
        AND AccountsPayable.InvoiceDocumenttype_BLART = 'RE'
        AND AccountsPayable.ClearingDate_AUGDT IS NULL,
        AccountsPayable.AmountInLocalCurrency_DMBTR,
        0
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS, AccountsPayable.DocFiscPeriod
    ) AS PeriodAP,

    SUM(
      IF(
        ## CORTEX-CUSTOMER: Please add relevant Account Type. Value 'K' represents 'Vendor'
        AccountsPayable.Accounttype_KOART = 'K'
        ## CORTEX-CUSTOMER: Please add relevant Document Type. Value 'RE' represents 'Invoice - Gross'
        AND AccountsPayable.InvoiceDocumenttype_BLART = 'RE'
        AND AccountsPayable.ClearingDate_AUGDT IS NULL,
        AccountsPayable.AmountInLocalCurrency_DMBTR,
        0
      )
    ) OVER (
      PARTITION BY AccountsPayable.Client_MANDT, AccountsPayable.CompanyCode_BUKRS
      ORDER BY AccountsPayable.DocFiscPeriod
    ) AS ClosingAP

  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.AccountsPayable`(KeyDate) AS AccountsPayable
  WHERE
    AccountsPayable.DocFiscPeriod <= AccountsPayable.KeyFiscPeriod
)

SELECT
  AccountsPayable.*,
  (AccountsPayable.ClosingAP - COALESCE(AccountsPayable.PeriodAP, 0)) AS OpeningAP,

  /* AccountsPayableTurnover */
  -- AccountsPayableTurnover = Total Purchases / ((OpeningAP + ClosingAP) / 2)
  SAFE_DIVIDE(
    AccountsPayable.TotalPurchases,
    ((AccountsPayable.ClosingAP * 2 - COALESCE(AccountsPayable.PeriodAP, 0)) / 2)
  ) AS AccountsPayableTurnover
FROM AccountsPayable
