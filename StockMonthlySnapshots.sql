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

CREATE TABLE IF NOT EXISTS `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.stock_monthly_snapshots`
(
mandt	STRING,
werks	STRING,
matnr	STRING,
charg	STRING,
lgort	STRING,
bukrs	STRING,
bwart	STRING,
insmk	STRING,
sobkz	STRING,
shkzg	STRING,
cal_year	INT64,
cal_month	INT64,
meins	STRING,
waers	STRING,
{% if sql_flavour == 's4' -%}
bstaus_sg STRING,
{% endif -%}
month_end_date	DATE,
total_monthly_movement_quantity	NUMERIC,
total_monthly_movement_amount NUMERIC,
amount_monthly_cumulative NUMERIC,
quantity_monthly_cumulative NUMERIC
);

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StockMonthlySnapshots`
OPTIONS(
description = "Stock Monthly Snapshots"
)
AS

{% if sql_flavour == 'ecc' or sql_flavour == 'union' -%}
{% include './ecc/StockMonthlySnapshots.sql' -%}
{% endif -%}
{% if sql_flavour == 's4' or sql_flavour == 'union' -%}
{% include './s4/StockMonthlySnapshots.sql' -%}
{% endif -%}
;
