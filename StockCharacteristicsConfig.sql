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

CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.stock_characteristics_config`
(
mandt	STRING,
insmk	STRING,
shkzg	STRING,
sobkz	STRING,
bwart	STRING,
stock_characteristic STRING
);

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StockCharacteristicsConfig`
OPTIONS(
description = "Stock Characteristics Config"
)
AS
{% include './ecc/StockCharacteristicsConfig.sql' -%}
;
{% endif -%}

{% if sql_flavour == 's4' or sql_flavour == 'union' -%}

CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ dataset_cdc_processed_s4 }}.stock_characteristics_config`
(
mandt	STRING,
bstaus_sg STRING,
sobkz	STRING,
stock_characteristic STRING
);

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.StockCharacteristicsConfig`
OPTIONS(
description = "Stock Characteristics Config"
)
AS
{% include './s4/StockCharacteristicsConfig.sql' -%}
;
{% endif -%}
