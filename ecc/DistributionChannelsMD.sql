SELECT
  TVTW.MANDT AS Client_MANDT,
  TVTW.VTWEG AS DistributionChannel_VTWEG,
  TVTWT.SPRAS AS Language_SPRAS,
  TVTWT.VTEXT AS DistributionChannelName_VTEXT
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.tvtw` AS TVTW
INNER JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.tvtwt` AS TVTWT
  ON
    TVTW.MANDT = TVTWT.MANDT
    AND TVTW.VTWEG = TVTWT.VTWEG
