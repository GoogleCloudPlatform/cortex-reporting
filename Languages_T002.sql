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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Languages_T002`
OPTIONS(
  description = "Languages table (T002)"
)
AS
SELECT
  T002.SPRAS AS LanguageKey_SPRAS,
  T002.LASPEZ AS LanguageSpecifications_LASPEZ,
  T002.LAHQ AS DegreeOfTranslationOfLanguage_LAHQ,
  T002.LAISO AS TwoCharacterSapLanguageCode_LAISO
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t002` AS T002
