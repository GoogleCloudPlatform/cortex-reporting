CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case3`(
  ip_mandt STRING, ip_periv STRING, ip_date DATE
) AS (
  (
    SELECT MIN(CONCAT(CAST(
          IF(Bdatj = '0000',
            CAST( EXTRACT(YEAR
                FROM
                Ip_Date)AS STRING),
            Bdatj) AS INT) + CAST(Reljr AS INT), "|", Poper))
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.t009b`
    WHERE
      Mandt = Ip_Mandt
      AND Periv = Ip_Periv
      AND CONCAT(
        IF(Bdatj = '0000',
          CAST( EXTRACT(YEAR
            FROM
            Ip_Date)AS STRING),
          Bdatj), Bumon, Butag) >= FORMAT_TIMESTAMP('%Y%m%d', Ip_Date) )
);
