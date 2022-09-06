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
CREATE TABLE IF NOT EXISTS {{ project_id_src  }}.{{ dataset_cdc_processed_ecc }}.holiday_calendar (
    HolidayDate	STRING,
    Description	STRING,
    CountryCode	STRING,
    Year	STRING,
    WeekDay	STRING,
    QuarterOfYear	INTEGER,
    Week	INTEGER
);
{% endif -%}

{% if sql_flavour == 's4' or sql_flavour == 'union' -%}
CREATE TABLE IF NOT EXISTS {{ project_id_src  }}.{{ dataset_cdc_processed_s4 }}.holiday_calendar (
    HolidayDate	STRING,
    Description	STRING,
    CountryCode	STRING,
    Year	STRING,
    WeekDay	STRING,
    QuarterOfYear	INTEGER,
    Week	INTEGER
);
{% endif -%}

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.HolidayCalendar`
OPTIONS(
description = "Holiday Calendar view"
)
AS
{% if sql_flavour == 'ecc' or sql_flavour == 'union' -%}
({% include './ecc/HolidayCalendar.sql' -%})
{% endif -%}

{% if sql_flavour == 'union' -%}
UNION ALL
{% endif -%}

{% if sql_flavour == 's4' or sql_flavour == 'union' -%}
({% include './s4/HolidayCalendar.sql' -%})
{% endif -%}
;
