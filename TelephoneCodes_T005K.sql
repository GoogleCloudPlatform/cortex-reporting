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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.TelephoneCodes_T005K`
OPTIONS(
  description = "Telephone Codes (T005)"
)
AS
SELECT
  T005K.MANDT AS Client_MANDT,
  T005K.LAND1 AS CountryKey_LAND1,
  T005K.TELEFFROM AS InternationalDialingCodeForTelephonefax_TELEFFROM,
  T005K.TELEFTO AS CountryTelephonefaxDiallingCode_TELEFTO,
  T005K.TELEFRM AS DigitToBeDeletedForCallsFromAbroad_TELEFRM,
  T005K.TELEXFROM AS ForeignDiallingCodeForTelex_TELEXFROM,
  T005K.TELEXTO AS ForeignDiallingCodeForTelex_TELEXTO,
  T005K.MOBILE_SMS AS Indicator_MobileTelephonesAreSmsEnabledByDefault_MOBILE_SMS
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t005k` AS T005K
