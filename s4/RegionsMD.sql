SELECT
  T005T.MANDT AS Client_MANDT,
  T005T.LAND1 AS CountryKey_LAND1,
  T005T.SPRAS AS Language_SPRAS,
  T005T.LANDX AS CountryName_LANDX
FROM
  -- `kittycorn-dev-infy.SAP_CDC_PROCESSED_ECC.t005` AS T005
  --JOIN
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.t005t` AS T005T
WHERE T005T.SPRAS = 'E'
--on
--  T005.MANDT=T005T.MANDT
 -- AND T005.LAND1 =T005T.LAND1
