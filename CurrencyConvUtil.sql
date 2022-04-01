#-- Copyright 2022 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CurrencyConvUtil`
OPTIONS(
  descriptiON = "Utility for Currency Conversion"
)
AS
WITH tcurf AS (
  SELECT mandt, kurst, fcurr, tcurr, gdatu, ffact, tfact, CAST( gdatu AS DECIMAL) AS decGDATU
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurf`
  WHERE kurst = 'M'
    AND mandt = '{{ mandt }}'
),

maxCURR AS (
  SELECT tc1.mandt, tc1.fcurr,
    tc1.tcurr, tc1.decGDATU AS decFROMGDATU,
    tc2.decGDATU AS decToGDATU
  FROM tcurf AS tc1
  INNER JOIN tcurf AS tc2
    ON tc1.mandt = tc2.mandt
  WHERE tc1.decGDATU > tc2.decGDATU
    AND tc1.mandt = '{{ mandt }}'
),

currUNI AS (
  SELECT mandt, fcurr, tcurr,
    decFROMGDATU AS FROMGDATU,
    MAX(decToGDATU) AS ToGDATU
  FROM maxCURR
  WHERE mandt = '{{ mandt }}'
  GROUP BY mandt, fcurr, tcurr, decFROMGDATU
  UNION ALL
  SELECT mandt, fcurr, tcurr, MIN(decGDATU) AS GDATU, MIN(decGDATU) AS minGDATU
  FROM tcurf
  WHERE mandt = '{{ mandt }}'
  GROUP BY mandt, fcurr, tcurr
)

SELECT curr.mandt AS Client_mandt, curr.fcurr AS SourceCurrency_FCURR,
  curr.tcurr AS TargetCurrency_TCURR,
  curr.FROMGDATU AS DateFROM,
  curr.ToGDATU AS DateTo,
  t.FFACT, t.TFACT,
  IF(x.currdec IS NULL, 2, x.currdec) AS FROMCurrDecimal,
  IF(y.currdec IS NULL, 2, y.currdec) AS toCurrDecimal,
  ( 10 * ( IF(y.currdec IS NULL, 2, y.currdec) - IF(x.currdec IS NULL, 2, x.currdec) ) ) * (t.FFACT / t.TFACT) AS ConversionFactor,
  ( 99999999 - curr.FROMGDATU ) AS validFROMDate,
  ( 99999998 - curr.ToGDATU ) AS validToDate
FROM currUNI AS curr
INNER JOIN tcurf AS t ON curr.mandt = t.mandt
  AND curr.fcurr = t.fcurr
  AND curr.tcurr = t.tcurr
  AND curr.FROMGDATU = t.decGDATU
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurx` AS x ON curr.fcurr = x.currkey
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurx` AS y ON curr.tcurr = y.currkey
  AND curr.mandt = '{{ mandt }}'
