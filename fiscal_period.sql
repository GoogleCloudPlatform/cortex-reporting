CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Period`(
  ip_mandt STRING, ip_periv STRING, ip_date DATE
) AS (
  (
    SELECT IF(COALESCE(LENGTH(Xkale),
          0) + COALESCE(LENGTH(Xjabh),
          0) = 0,
        'CASE3',
        IF(Xkale IS NOT NULL,
          'CASE1',
          'CASE2'))
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.t009`
    WHERE
      Mandt = Ip_Mandt
      AND Periv = Ip_Periv )
);
