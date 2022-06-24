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
CREATE TABLE IF NOT EXISTS {{ project_id_src }}.{{ dataset_cdc_processed }}.weather_daily
(
  country STRING,
  postcode STRING,
  date DATE,
  min_temp FLOAT64,
  max_temp FLOAT64,
  value_type STRING,
  insert_timestamp TIMESTAMP,
  update_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{ project_id_src }}.{{ dataset_cdc_processed }}.weather_weekly
AS
SELECT
  country,
  postcode,
  DATE_TRUNC(date, WEEK(MONDAY)) AS week_start_date,
  AVG(min_temp) AS avg_min_temp,
  AVG(max_temp) AS avg_max_temp,
  MIN(min_temp) AS min_temp,
  MAX(max_temp) AS max_temp
FROM {{ project_id_src }}.{{ dataset_cdc_processed }}.weather_daily 
GROUP BY country, postcode, week_start_date;

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Weather`
AS
SELECT
  country as Country,
  postcode as PostCode,
  week_start_date as WeekStartDate,
  avg_min_temp as AvgMinTemp,
  avg_max_temp as AvgMaxTemp,
  min_temp as MinTemp,
  max_temp as MaxTemp
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.weather_weekly`
