CREATE OR REPLACE FUNCTION `{{ project_id_src }}.{{ dataset_reporting_tgt }}.Currency_Decimal`(ip_curr STRING) AS ((
  SELECT
    currdec
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurx`
  WHERE currkey = ip_curr
));
