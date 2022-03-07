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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.UoMMD`
OPTIONS(
  description = "Unit Of Measure master data"
)
AS
SELECT
  T006.MANDT AS Client_MANDT,
  T006.MSEHI AS UnitOfMeasurement_MSEHI,
  T006.KZEX3 AS ThreeCharIndicatorForExternalUnitOfMeasurement_KZEX3,
  T006.KZEX6 AS SixCharIdForExternalUnitOfMeasurement_KZEX6,
  T006T.SPRAS AS LanguageKey_SPRAS, T006T.TXDIM AS DimensionText_TXDIM,
  T006A.MSEHT AS UnitOfMeasurementText__maximum10Characters___MSEHT,
  T006A.MSEHL AS UnitOfMeasurementText__maximum30Characters___MSEHL
#T006.ANDEC AS NoOfDecimalPlacesForRounding_ANDEC,  T006.KZKEH AS CommercialMeasurementUnitId_KZKEH,  T006.KZWOB AS ValueBasedCommitmentIndicator_KZWOB,  T006.KZ1EH AS Indicator__1__Unit__indicatorNotYetDefined___KZ1EH,
#T006.KZ2EH AS Indicator__2__Unit__indicatorNotYetDefined___KZ2EH,  T006.DIMID AS DimensionKey_DIMID,  T006.ZAEHL AS NumeratorForConversionToSiUnit_ZAEHL,  T006.NENNR AS DenominatorForConversionIntoSiUnit_NENNR,
#T006.EXP10 AS BaseTenExponentForConversionToSiUnit_EXP10,  T006.ADDKO AS AdditiveConstantForConversionToSiUnit_ADDKO,  T006.EXPON AS BaseTenExponentForFloatingPointDisplay_EXPON,
#T006.DECAN AS NumberOfDecimalPlacesForNumberDisplay_DECAN,  T006.ISOCODE AS IsoCodeForUnitOfMeasurement_ISOCODE,  T006.PRIMARY AS SelectionFieldForConversionFromIsoCodeToIntCode_PRIMARY,  
#T006.TEMP_VALUE AS Temperature_TEMP_VALUE,
#T006.TEMP_UNIT AS TemperatureUnit_TEMP_UNIT, T006.FAMUNIT AS UnitOfMeasurementFamily_FAMUNIT, T006.PRESS_VAL AS PressureValue_PRESS_VAL, T006.PRESS_UNIT AS UnitOfPressure_PRESS_UNIT
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t006` AS t006
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.t006t` AS t006t
  ON t006.mandt = t006t.mandt
    AND t006.dimid = t006t.dimid
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.t006a` AS t006a
  ON t006.mandt = t006a.mandt
    AND t006a.spras = t006t.spras
    AND t006a.msehi = t006.msehi
