--This query groups the data by Client, MaterialNumber, Plant, CalYear, CalMonth, Target Currency
--## CORTEX-CUSTOMER: Please consider materializing this view for better performance
--and cost as this is pretty heavy in run-time calculation
WITH
  CurrencyConversion AS (
    SELECT
      Client_MANDT, FromCurrency_FCURR, ToCurrency_TCURR, ConvDate, ExchangeRate_UKURS
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CurrencyConversion`
    WHERE
      ToCurrency_TCURR {{ currency }}
      --##CORTEX-CUSTOMER Modify the exchange rate type based on your requirement
      AND ExchangeRateType_KURST = 'M'
  ),

  CurrentStock AS (
    SELECT
      StockMonthlySnapshots.Client_MANDT,
      StockMonthlySnapshots.MaterialNumber_MATNR,
      StockMonthlySnapshots.Plant_WERKS,
      StockMonthlySnapshots.MaterialType_MTART,
      SlowMovingThreshold.ThresholdValue,
      StockMonthlySnapshots.DescriptionOfMaterialType_MTBEZ,
      StockMonthlySnapshots.MaterialText_MAKTX,
      StockMonthlySnapshots.MaterialGroup_MATKL,
      StockMonthlySnapshots.MaterialGroupName_WGBEZ,
      StockMonthlySnapshots.Plant_Name2_NAME2,
      StockMonthlySnapshots.CompanyCode_BUKRS,
      StockMonthlySnapshots.CompanyText_BUTXT,
      StockMonthlySnapshots.CountryKey_LAND1,
      StockMonthlySnapshots.LanguageKey_SPRAS,
      StockMonthlySnapshots.BaseUnitOfMeasure_MEINS,
      StockMonthlySnapshots.CurrencyKey_WAERS,
      StockMonthlySnapshots.FiscalYear,
      StockMonthlySnapshots.FiscalPeriod,
      StockMonthlySnapshots.CalYear,
      StockMonthlySnapshots.CalMonth,
      IF(
        StockMonthlySnapshots.MonthEndDate = LAST_DAY(CURRENT_DATE),
        CURRENT_DATE,
        StockMonthlySnapshots.MonthEndDate) AS MonthEndDate,
      IF(
        MaterialLedger.ValueOfTotalValuatedStock_SALK3 IS NULL,
        LAST_VALUE(MaterialLedger.ValueOfTotalValuatedStock_SALK3 IGNORE NULLS) OVER (
          ORDER BY StockMonthlySnapshots.MaterialNumber_MATNR, StockMonthlySnapshots.Plant_WERKS,
            StockMonthlySnapshots.MonthEndDate),
        MaterialLedger.ValueOfTotalValuatedStock_SALK3) AS ValueOfTotalValuatedStock_SALK3,
      IF(
        MaterialLedger.StandardCost_STPRS IS NULL,
        LAST_VALUE(MaterialLedger.StandardCost_STPRS IGNORE NULLS) OVER (
          ORDER BY StockMonthlySnapshots.MaterialNumber_MATNR,
            StockMonthlySnapshots.Plant_WERKS,
            StockMonthlySnapshots.MonthEndDate),
        MaterialLedger.StandardCost_STPRS) AS StandardCost_STPRS,
      IF(
        MaterialLedger.MovingAveragePrice_VERPR IS NULL,
        LAST_VALUE(MaterialLedger.MovingAveragePrice_VERPR IGNORE NULLS) OVER (
          ORDER BY StockMonthlySnapshots.MaterialNumber_MATNR,
            StockMonthlySnapshots.Plant_WERKS,
            StockMonthlySnapshots.MonthEndDate),
        MaterialLedger.MovingAveragePrice_VERPR) AS MovingAveragePrice_VERPR,
      SUM(StockMonthlySnapshots.QuantityMonthlyCumulative) AS QuantityMonthlyCumulative,
      SUM(StockMonthlySnapshots.AmountMonthlyCumulative) AS AmountMonthlyCumulative,
      SUM(StockMonthlySnapshots.StockOnHand) AS StockOnHand,
      SUM(StockMonthlySnapshots.StockOnHandValue) AS StockOnHandValue,
      SUM(StockMonthlySnapshots.QuantityIssuedToDelivery) AS QuantityIssuedToDelivery,
      SUM(StockMonthlySnapshots.TotalConsumptionQuantity) AS TotalConsumptionQuantity
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StockMonthlySnapshots` AS StockMonthlySnapshots
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialLedger` AS MaterialLedger
      ON
        StockMonthlySnapshots.Client_MANDT = MaterialLedger.Client_MANDT
        AND StockMonthlySnapshots.MaterialNumber_MATNR = MaterialLedger.MaterialNumber_MATNR
        AND StockMonthlySnapshots.Plant_WERKS = MaterialLedger.ValuationArea_BWKEY
        AND StockMonthlySnapshots.FiscalYear = MaterialLedger.FiscalYear_LFGJA
        AND StockMonthlySnapshots.FiscalPeriod = MaterialLedger.PostingPeriod_LFMON
        AND MaterialLedger.ValuationType_BWTAR = ''
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SlowMovingThreshold` AS SlowMovingThreshold
      ON
        StockMonthlySnapshots.Client_MANDT = SlowMovingThreshold.Client_MANDT
        AND StockMonthlySnapshots.MaterialType_MTART = SlowMovingThreshold.MaterialType_MTART
    GROUP BY
      StockMonthlySnapshots.Client_MANDT,
      StockMonthlySnapshots.MaterialNumber_MATNR,
      StockMonthlySnapshots.Plant_WERKS,
      StockMonthlySnapshots.MaterialType_MTART,
      SlowMovingThreshold.ThresholdValue,
      StockMonthlySnapshots.DescriptionOfMaterialType_MTBEZ,
      StockMonthlySnapshots.MaterialText_MAKTX,
      StockMonthlySnapshots.MaterialGroup_MATKL,
      StockMonthlySnapshots.MaterialGroupName_WGBEZ,
      StockMonthlySnapshots.Plant_Name2_NAME2,
      StockMonthlySnapshots.CompanyCode_BUKRS,
      StockMonthlySnapshots.CompanyText_BUTXT,
      StockMonthlySnapshots.CountryKey_LAND1,
      StockMonthlySnapshots.LanguageKey_SPRAS,
      MaterialLedger.ValueOfTotalValuatedStock_SALK3,
      MaterialLedger.StandardCost_STPRS,
      MaterialLedger.MovingAveragePrice_VERPR,
      StockMonthlySnapshots.BaseUnitOfMeasure_MEINS,
      StockMonthlySnapshots.CurrencyKey_WAERS,
      StockMonthlySnapshots.FiscalYear,
      StockMonthlySnapshots.FiscalPeriod,
      StockMonthlySnapshots.CalYear,
      StockMonthlySnapshots.CalMonth,
      StockMonthlySnapshots.MonthEndDate
  ),

  Inventory AS (
    SELECT
      CurrentStock.Client_MANDT,
      CurrentStock.MaterialNumber_MATNR,
      CurrentStock.Plant_WERKS,
      CurrentStock.MaterialType_MTART,
      CurrentStock.ThresholdValue,
      CurrentStock.DescriptionOfMaterialType_MTBEZ,
      CurrentStock.MaterialText_MAKTX,
      CurrentStock.MaterialGroup_MATKL,
      CurrentStock.MaterialGroupName_WGBEZ,
      CurrentStock.Plant_Name2_NAME2,
      CurrentStock.CompanyCode_BUKRS,
      CurrentStock.CompanyText_BUTXT,
      CurrentStock.CountryKey_LAND1,
      CurrentStock.LanguageKey_SPRAS,
      CurrentStock.ValueOfTotalValuatedStock_SALK3,
      CurrentStock.StandardCost_STPRS,
      CurrentStock.MovingAveragePrice_VERPR,
      CurrentStock.BaseUnitOfMeasure_MEINS,
      CurrentStock.CurrencyKey_WAERS,
      CurrentStock.FiscalYear,
      CurrentStock.FiscalPeriod,
      CurrentStock.CalYear,
      CurrentStock.CalMonth,
      CurrentStock.MonthEndDate,
      CurrentStock.QuantityMonthlyCumulative,
      CurrentStock.AmountMonthlyCumulative,
      CurrentStock.StockOnHand,
      CurrentStock.StockOnHandValue,
      CurrentStock.QuantityIssuedToDelivery,
      CurrentStock.TotalConsumptionQuantity,
      CurrentStock.QuantityIssuedToDelivery * CurrentStock.StandardCost_STPRS AS CostOfGoodsSoldByMonth,

      -- TotalConsumptionQuantity for past 12 months
      IF(CurrentStock.MonthEndDate BETWEEN LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH))
        AND LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)),
        SUM(CurrentStock.TotalConsumptionQuantity) OVER(
          PARTITION BY CurrentStock.Client_MANDT, CurrentStock.MaterialNumber_MATNR,
            CurrentStock.Plant_WERKS, CurrentStock.LanguageKey_SPRAS,
            CurrentStock.MonthEndDate BETWEEN LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))
            AND LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH))),
        0) AS TotalConsumptionQuantityForPastYear,

      -- TotalConsumptionQuantity AS Of Today
      IF(CurrentStock.MonthEndDate BETWEEN LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH))
        AND LAST_DAY(CURRENT_DATE),
        SUM(CurrentStock.TotalConsumptionQuantity) OVER(
          PARTITION BY CurrentStock.Client_MANDT, CurrentStock.MaterialNumber_MATNR,
            CurrentStock.Plant_WERKS, CurrentStock.LanguageKey_SPRAS,
            CurrentStock.MonthEndDate BETWEEN LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH))
            AND LAST_DAY(CURRENT_DATE)),
        0) AS TotalConsumptionQuantityForPastYearTillToday,

      --  Demand Per Day For Past 12 Months To Current Date
      IF(CurrentStock.MonthEndDate BETWEEN LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH))
        AND LAST_DAY(CURRENT_DATE),
        SUM(TotalConsumptionQuantity / (365 + DATE_DIFF(CAST(CURRENT_DATE AS DATE),
          CAST(DATE_TRUNC(CURRENT_DATE, MONTH) AS DATE), DAY))) OVER(
          PARTITION BY CurrentStock.Client_MANDT, CurrentStock.MaterialNumber_MATNR,
            CurrentStock.Plant_WERKS, LanguageKey_SPRAS,
            CurrentStock.MonthEndDate BETWEEN LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH))
            AND LAST_DAY(CURRENT_DATE)),
        0) AS DemandPerDayForPastYearTillToday,

      -- Inventory For Each Month
      SUM(CurrentStock.ValueOfTotalValuatedStock_SALK3) OVER(
        PARTITION BY CurrentStock.Client_MANDT, CurrentStock.MaterialNumber_MATNR,
          CurrentStock.Plant_WERKS, CurrentStock.LanguageKey_SPRAS
        ORDER BY CurrentStock.CalYear, CurrentStock.CalMonth ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
      ) AS InventoryByMonth,

      -- Avg Inventory For Each Month
      SUM(CurrentStock.ValueOfTotalValuatedStock_SALK3 / 2) OVER(
        PARTITION BY CurrentStock.Client_MANDT, CurrentStock.MaterialNumber_MATNR,
          CurrentStock.Plant_WERKS, CurrentStock.LanguageKey_SPRAS
        ORDER BY CurrentStock.CalYear, CurrentStock.CalMonth ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
      ) AS AvgInventoryByMonth,

      -- Inventory Value
      IF(CurrentStock.MaterialType_MTART IN ('FERT', 'HALB'),
        CurrentStock.QuantityMonthlyCumulative * CurrentStock.StandardCost_STPRS,
        IF(CurrentStock.MaterialType_MTART IN ('ROH', 'HIBE'),
          CurrentStock.QuantityMonthlyCumulative * CurrentStock.MovingAveragePrice_VERPR,
          0)
      ) AS InventoryValue
    FROM
      CurrentStock
  )

SELECT
  Inventory.Client_MANDT,
  Inventory.MaterialNumber_MATNR,
  Inventory.Plant_WERKS,
  Inventory.MaterialType_MTART,
  Inventory.DescriptionOfMaterialType_MTBEZ,
  Inventory.MaterialText_MAKTX,
  Inventory.MaterialGroup_MATKL,
  Inventory.MaterialGroupName_WGBEZ,
  Inventory.Plant_Name2_NAME2,
  Inventory.CompanyCode_BUKRS,
  Inventory.CompanyText_BUTXT,
  Inventory.CountryKey_LAND1,
  Inventory.LanguageKey_SPRAS,
  Inventory.FiscalYear,
  Inventory.FiscalPeriod,
  Inventory.CalYear,
  Inventory.CalMonth,
  Inventory.MonthEndDate,
  Inventory.QuantityMonthlyCumulative,
  Inventory.BaseUnitOfMeasure_MEINS,
  Inventory.AmountMonthlyCumulative,
  Inventory.CurrencyKey_WAERS,
  Inventory.StockOnHand,
  Inventory.StockOnHandValue,
  Inventory.QuantityIssuedToDelivery,
  Inventory.TotalConsumptionQuantity,
  Inventory.ValueOfTotalValuatedStock_SALK3,
  Inventory.StandardCost_STPRS,
  Inventory.MovingAveragePrice_VERPR,
  Inventory.TotalConsumptionQuantityForPastYear,
  Inventory.TotalConsumptionQuantityForPastYearTillToday,
  Inventory.DemandPerDayForPastYearTillToday,
  Inventory.CostOfGoodsSoldByMonth,
  Inventory.InventoryValue,
  Inventory.ThresholdValue,
  Inventory.InventoryByMonth,
  Inventory.AvgInventoryByMonth,
  -- The following columns are having amount/prices in target currency.
  CurrencyConversion.ExchangeRate_UKURS,
  CurrencyConversion.ToCurrency_TCURR AS TargetCurrency_TCURR,
  Inventory.AmountMonthlyCumulative * CurrencyConversion.ExchangeRate_UKURS AS AmountMonthlyCumulativeInTargetCurrency,
  Inventory.StockOnHandValue * CurrencyConversion.ExchangeRate_UKURS AS StockOnHandValueInTargetCurrency,
  Inventory.StandardCost_STPRS * CurrencyConversion.ExchangeRate_UKURS AS StandardCostInTargetCurrency_STPRS,
  Inventory.MovingAveragePrice_VERPR * CurrencyConversion.ExchangeRate_UKURS AS MovingAveragePriceInTargetCurrency_VERPR,
  Inventory.CostOfGoodsSoldByMonth * CurrencyConversion.ExchangeRate_UKURS AS CostofGoodsSoldInTargetCurrency,
  Inventory.InventoryValue * CurrencyConversion.ExchangeRate_UKURS AS InventoryValueInTargetCurrency,
  Inventory.InventoryByMonth * CurrencyConversion.ExchangeRate_UKURS AS InventoryByMonthInTargetCurrency,
  Inventory.AvgInventoryByMonth * CurrencyConversion.ExchangeRate_UKURS AS AvgInventoryByMonthInTargetCurrency,
  Inventory.ValueOfTotalValuatedStock_SALK3 * CurrencyConversion.ExchangeRate_UKURS AS ValueOfTotalValuatedStockInTargetCurrency_SALK3,

  -- Slow Moving Inventory In Target Currency
  IF(COALESCE(
      IF(Inventory.MonthEndDate = LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)),
        SAFE_DIVIDE((Inventory.TotalConsumptionQuantityForPastYear * 100), Inventory.StockOnHand),
        0),
      0) < Inventory.ThresholdValue,
    IF(Inventory.MonthEndDate = LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)),
      (Inventory.StockOnHandValue * CurrencyConversion.ExchangeRate_UKURS),
      0),
    0) AS SlowMovingInventoryAsOfPreviousMonthInTargetCurrency,

  -- Days Of Supply
  COALESCE(
    IF(Inventory.MonthEndDate = CURRENT_DATE,
      SAFE_DIVIDE(Inventory.StockOnHand, Inventory.DemandPerDayForPastYearTillToday),
      0),
    0) AS DaysOfSupplyAsOfToday,

  -- Slow Moving Indicator
  IF(
    COALESCE(
      IF(Inventory.MonthEndDate = LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)),
        SAFE_DIVIDE((Inventory.TotalConsumptionQuantityForPastYear * 100), Inventory.StockOnHand),
        0),
      0) < Inventory.ThresholdValue,
    COALESCE(
      IF(Inventory.MonthEndDate = LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)),
        SAFE_DIVIDE((Inventory.TotalConsumptionQuantityForPastYear * 100), Inventory.StockOnHand), 0),
      0),
    0) AS SlowMovingIndicatorAsOfPreviousMonth,

  -- Slow Moving Inventory In Source Currency
  IF(
    COALESCE(
      IF(Inventory.MonthEndDate = LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)),
        SAFE_DIVIDE((Inventory.TotalConsumptionQuantityForPastYear * 100), Inventory.StockOnHand),
        0),
      0) < Inventory.ThresholdValue,
    IF(Inventory.MonthEndDate = LAST_DAY(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)),
      Inventory.StockOnHandValue,
      0),
    0) AS SlowMovingInventoryAsOfPreviousMonthInSourceCurrency,

  -- Inventory Turn
  COALESCE(
    SAFE_DIVIDE(Inventory.CostOfGoodsSoldByMonth, Inventory.AvgInventoryByMonth),
    0) AS InventoryTurnByMonth

FROM
  Inventory
LEFT JOIN
  CurrencyConversion
  ON
    Inventory.Client_MANDT = CurrencyConversion.Client_MANDT
    AND Inventory.CurrencyKey_WAERS = CurrencyConversion.FromCurrency_FCURR
    AND Inventory.MonthEndDate = CurrencyConversion.ConvDate
