SELECT
  StockCharacteristicsConfig.MANDT AS Client_MANDT,
  StockCharacteristicsConfig.INSMK AS StockType_INSMK,
  StockCharacteristicsConfig.SHKZG AS Debit_CreditIndicator_SHKZG,
  StockCharacteristicsConfig.SOBKZ AS SpecialStockIndicator_SOBKZ,
  StockCharacteristicsConfig.BWART AS MovementType_BWART,
  StockCharacteristicsConfig.Stock_Characteristic AS StockCharacteristic
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.stock_characteristics_config` AS StockCharacteristicsConfig
