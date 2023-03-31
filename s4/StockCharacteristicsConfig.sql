SELECT
  StockCharacteristicsConfig.MANDT AS Client_MANDT,
  StockCharacteristicsConfig.SOBKZ AS SpecialStockIndicator_SOBKZ,
  StockCharacteristicsConfig.BSTAUS_SG AS StockCharacteristic_BSTAUS_SG,
  StockCharacteristicsConfig.stock_characteristic AS StockCharacteristic
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.stock_characteristics_config` AS StockCharacteristicsConfig
