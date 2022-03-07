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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.POSchedule`
OPTIONS(
  description = "Purchase Order Schedule"
)
AS
SELECT
  EKET.MANDT AS Client_MANDT, EKET.EBELN AS PurchasingDocumentNumber_EBELN,
  EKET.EBELP AS ItemNumberOfPurchasingDocument_EBELP,
  EKET.ETENR AS DeliveryScheduleLineCounter_ETENR,
  EKET.EINDT AS ItemDeliveryDate_EINDT,
  EKET.SLFDT AS StatisticsRelevantDeliveryDate_SLFDT,
  EKET.LPEIN AS CategoryOfDeliveryDate_LPEIN,
  EKET.MENGE AS ScheduledQuantity_MENGE,
  EKET.AMENG AS PreviousQuantity__deliveryScheduleLines___AMENG,
  EKET.WEMNG AS QuantityOfGoodsReceived_WEMNG, EKET.WAMNG AS IssuedQuantity_WAMNG,
  EKET.UZEIT AS DeliveryDateTimeSpot_UZEIT, EKET.BANFN AS PurchaseRequisitionNumber_BANFN,
  EKET.BNFPO AS ItemNumberOfPurchaseRequisition_BNFPO,
  EKET.ESTKZ AS CreationIndicator__purchaseRequisitionscheduleLines___ESTKZ,
  EKET.QUNUM AS NumberOfQuotaArrangement_QUNUM,
  EKET.QUPOS AS QuotaArrangementItem_QUPOS,
  EKET.MAHNZ AS NoOfRemindersexpeditersForScheduleLine_MAHNZ,
  EKET.BEDAT AS OrderDateOfScheduleLine_BEDAT,
  EKET.RSNUM AS NumberOfReservationdependentRequirements_RSNUM,
  EKET.SERNR AS BomExplosionNumber_SERNR, EKET.FIXKZ AS ScheduleLineIsfixed_FIXKZ,
  EKET.GLMNG AS QuantityDelivered__stockTransfer___GLMNG,
  EKET.DABMG AS QuantityReduced__mrp___DABMG,
  EKET.CHARG AS BatchNumber_CHARG, EKET.LICHA AS VendorBatchNumber_LICHA,
  EKET.CHKOM AS Components_CHKOM,
  EKET.VERID AS ProductionVersion_VERID, EKET.ABART AS SchedulingAgreementReleaseType_ABART,
  EKET.MNG02 AS CommittedQuantity_MNG02, EKET.DAT01 AS CommittedDate_DAT01,
  EKET.ALTDT AS PreviousDeliveryDate_ALTDT,
  EKET.AULWE AS RouteSchedule_AULWE, EKET.MBDAT AS MaterialAvailabilityDate_MBDAT,
  EKET.MBUHR AS MaterialStagingTime_MBUHR, EKET.LDDAT AS LoadingDate_LDDAT,
  EKET.LDUHR AS LoadingTime__localTimeRelatingToAShippingPoint___LDUHR,
  EKET.TDDAT AS TransportationPlanningDate_TDDAT,
  EKET.TDUHR AS TranspPlanningTime__local_TDUHR, EKET.WADAT AS GoodsIssueDate_WADAT,
  EKET.WAUHR AS TimeOfGoodsIssue__local_RelatingToAPlant___WAUHR,
  EKET.ELDAT AS GoodsReceiptEndDate_ELDAT, EKET.ELUHR AS GoodsReceiptEndTime__local__ELUHR,
  EKET.ANZSN AS NumberOfSerialNumbers_ANZSN,
  EKET.NODISP AS Ind_ReservNotApplicableToMrpPurcReqNotCreated_NODISP,
  EKET.GEO_ROUTE AS DescriptionOfAGeographicalRoute_GEO_ROUTE,
  EKET.ROUTE_GTS AS RouteCodeForSapGlobalTradeServices_ROUTE_GTS,
  EKET.GTS_IND AS GoodsTrafficType_GTS_IND, EKET.TSP AS ForwardingAgent_TSP,
  EKET.CD_LOCNO AS LocationNumberInApo_CD_LOCNO, EKET.CD_LOCTYPE AS ApoLocationType_CD_LOCTYPE,
  EKET.HANDOVERDATE AS HandoverDateAtTheHandoverLocation_HANDOVERDATE,
  EKET.HANDOVERTIME AS HandoverTimeAtTheHandoverLocation_HANDOVERTIME,
  EKET.FSH_RALLOC_QTY AS ArunRequirementAllocatedQuantity_FSH_RALLOC_QTY,
  EKET.FSH_SALLOC_QTY AS AllocatedStockQuantity_FSH_SALLOC_QTY,
  EKET.FSH_OS_ID AS OrderSchedulingGroupId_FSH_OS_ID, EKET.KEY_ID AS UniqueNumberOfBudget_KEY_ID,
  EKET.OTB_VALUE AS RequiredBudget_OTB_VALUE,
  EKET.OTB_CURR AS OtbCurrency_OTB_CURR,
  EKET.OTB_RES_VALUE AS ReservedBudgetForOtbRelevantPurchasingDocument_OTB_RES_VALUE,
  EKET.OTB_SPEC_VALUE AS SpecialReleaseBudget_OTB_SPEC_VALUE,
  EKET.SPR_RSN_PROFILE AS ReasonProfileForOtbSpecialRelease_SPR_RSN_PROFILE,
  EKET.BUDG_TYPE AS BudgetType_BUDG_TYPE, EKET.OTB_STATUS AS OtbCheckStatus_OTB_STATUS,
  EKET.OTB_REASON AS ReasonIndicatorForOtbCheckStatus_OTB_REASON,
  EKET.CHECK_TYPE AS TypeOfOtbCheck_CHECK_TYPE, EKET.DL_ID AS DatelineId__guid___DL_ID,
  EKET.HANDOVER_DATE AS TransferDate_HANDOVER_DATE,
  EKET.NO_SCEM AS PurchaseOrderNotTransferredToScem_NO_SCEM,
  EKET.DNG_DATE AS CreationDateOfReminderMessageRecord_DNG_DATE,
  EKET.DNG_TIME AS CreationTimeOfReminderMessageRecord_DNG_TIME,
  EKET.CNCL_ANCMNT_DONE AS CancellationThreatMade_CNCL_ANCMNT_DONE,
  EKET.DATESHIFT_NUMBER AS NumberOfCurrentDateShifts_DATESHIFT_NUMBER
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.eket` AS EKET
