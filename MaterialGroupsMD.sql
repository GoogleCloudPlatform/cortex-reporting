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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialGroupsMD`
OPTIONS(
  description = "Material Groups Master Data"
)
AS
SELECT
  t023.MANDT AS Client_MANDT,
  t023.MATKL AS MaterialGroup_MATKL,
  t023.SPART AS Division_SPART,
  t023.WWGDA AS ReferenceGroupRefMaterial_WWGDA,
  t023.WWGPA AS GroupMaterial_WWGPA,
  t023.ABTNR AS DepartmentNumber_ABTNR,
  t023.BEGRU AS AuthorizationGroup_BEGRU,
  t023.GEWEI AS DefaultUnitofWeight_GEWEI,
  t023.BKLAS AS ValuationClass_BKLAS,
  t023.EKWSL AS PurchasingValueKey_EKWSL,
  t023.ANLKL AS AssetClass_ANLKL,
  t023.PRICE_GROUP AS PriceLevelGroup_PRICE_GROUP,
  t023t.wgbez AS MaterialGroupName_WGBEZ,
  t023t.spras AS Language_SPRAS
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t023` AS t023
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.t023t` AS t023t
  ON t023.mandt = t023t.mandt
    AND t023.matkl = t023t.matkl
