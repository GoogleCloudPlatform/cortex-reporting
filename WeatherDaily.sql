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
{% if sql_flavour == 'ecc' or sql_flavour == 'union' -%}
CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.weather_daily`
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
{% endif -%}

{% if sql_flavour == 's4' or sql_flavour == 'union' -%}
CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.weather_daily`
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

{% endif -%}

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.WeatherDaily`
OPTIONS(
description = "Weather Daily view"
)
AS
{% if sql_flavour == 'ecc' or sql_flavour == 'union' -%}
({% include './ecc/WeatherDaily.sql' -%})
{% endif -%}

{% if sql_flavour == 'union' -%}
UNION ALL
{% endif -%}

{% if sql_flavour == 's4' or sql_flavour == 'union' -%}
({% include './s4/WeatherDaily.sql' -%})
{% endif -%}
;