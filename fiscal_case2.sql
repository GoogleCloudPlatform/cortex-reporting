CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Fiscal_Case2`(
  ip_mandt STRING, ip_periv STRING, ip_date DATE
) AS (
  (
    SELECT CONCAT(CAST(Bdatj AS INT) + CAST(Reljr AS INT), "|", Poper)
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.t009b`
    WHERE
      Mandt = Ip_Mandt
      AND Periv = Ip_Periv
      AND Bdatj = CAST(EXTRACT(YEAR
        FROM
        Ip_Date) AS STRING)
      AND Bumon = CAST(CAST( Ip_Date AS STRING FORMAT('MM')) AS STRING ))
);
