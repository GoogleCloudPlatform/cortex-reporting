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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.MaterialTypesMD`
OPTIONS(
  description = "Material Types and texts"
)
AS
SELECT
  T134.MANDT AS Client_MANDT,
  T134.MTART AS MaterialType_MTART,
  T134.MTREF AS ReferenceMaterialType_MTREF,
  T134.MBREF AS ScreenReferenceDependingOnTheMaterialType_MBREF,
  T134T.SPRAS AS LanguageKey_SPRAS, T134T.MTBEZ AS DescriptionOfMaterialType_MTBEZ
#T134.FLREF AS FieldReferenceForMaterialMaster_FLREF,
#T134.NUMKI AS NumberRange_NUMKI,  T134.NUMKE AS NumberRange_NUMKE,  T134.ENVOP AS ExternalNumberAssignmentWithoutValidation_ENVOP,  T134.BSEXT AS ExternalPurchaseOrdersAllowed_BSEXT,
#T134.BSINT AS InternalPurchaseOrdersAllowed_BSINT,  T134.PSTAT AS MaintenanceStatus_PSTAT,  T134.KKREF AS AccountCategoryReference_KKREF,  T134.VPRSV AS PriceControlIndicator_VPRSV,  T134.KZVPR AS PriceControlMandatory_KZVPR,
#T134.VMTPO AS DefaultValueForMaterialItemCategoryGroup_VMTPO,  T134.EKALR AS MaterialIsCostedWithQuantityStructure_EKALR,  T134.KZGRP AS GroupingIndicator_KZGRP,  T134.KZKFG AS ConfigurableMaterial_KZKFG,
#T134.BEGRU AS AuthorizationGroupInTheMaterialMaster_BEGRU,  T134.KZPRC AS MaterialMasterRecordForAProcess_KZPRC,  T134.KZPIP AS PipelineHandlingMandatory_KZPIP,  T134.PRDRU AS DisplayPriceOnCashRegisterDisplayAndPrintOnReceipt_PRDRU,
#T134.ARANZ AS DisplayMaterialOnCashRegisterDisplay_ARANZ,  T134.WMAKG AS MaterialTypeId_WMAKG,  T134.IZUST AS InitialStatusOfANewBatch_IZUST,  T134.ARDEL AS TimeInDaysUntilAMaterialIsDeleted_ARDEL,
#T134.KZMPN AS ManufacturerPart_KZMPN,  T134.MSTAE AS CrossPlantMaterialStatus_MSTAE,  T134.CCHIS AS Control__time__OfHistoryRequirement_Material_CCHIS,  T134.CTYPE AS ClassType_CTYPE,  T134.CLASS AS ClassNumber_CLASS,
#T134.CHNEU AS BatchCreationControl__automaticmanual___CHNEU,  T134.VTYPE AS VersionCategory_VTYPE,  T134.VNUMKI AS NumberRange_VNUMKI,  T134.VNUMKE AS NumberRange_VNUMKE,  T134.KZRAC AS ReturnablePackagingLogisticsIsMandatory_KZRAC,
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.t134` AS t134
INNER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.t134t` AS t134t
  ON t134.mandt = t134t.mandt AND t134.mtart = t134t.mtart
