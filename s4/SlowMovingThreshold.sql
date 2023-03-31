SELECT
  SlowMovingThreshold.MANDT AS Client_MANDT,
  SlowMovingThreshold.MTART AS MaterialType_MTART,
  SlowMovingThreshold.threshold_value AS ThresholdValue
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.slow_moving_threshold` AS SlowMovingThreshold
