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
CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Currency_Conversion_ecc`(
  ip_mandt STRING, ip_kurst STRING, ip_fcurr STRING, ip_tcurr STRING, ip_date DATE, ip_amount NUMERIC
) AS ((
{% include './ecc/currency_conversion.sql' -%}
));
{% endif -%}


{% if sql_flavour == 's4' or sql_flavour == 'union' -%}
CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Currency_Conversion_s4`(
  ip_mandt STRING, ip_kurst STRING, ip_fcurr STRING, ip_tcurr STRING, ip_date DATE, ip_amount NUMERIC
) AS ((
{% include './s4/currency_conversion.sql' -%}
));
{% endif -%}


CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Currency_Conversion`(
  ip_mandt STRING, ip_kurst STRING, ip_fcurr STRING, ip_tcurr STRING, ip_date DATE, ip_amount NUMERIC
) AS
((
{% if sql_flavour == 'ecc' or sql_flavour == 'union' -%}
({% include './ecc/currency_conversion.sql' -%})
{% endif -%}

{% if sql_flavour == 'union' -%}
UNION ALL
{% endif -%}

{% if sql_flavour == 's4' or sql_flavour == 'union' -%}
({% include './s4/currency_conversion.sql' -%})
{% endif -%}
))