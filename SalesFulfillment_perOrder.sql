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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesFulfillment_perOrder`
OPTIONS(
  description = "Sales Orders Fulfillment per Order and Plant "
)
AS
WITH SO AS (
  SELECT SO.Client_MANDT, SO.SalesOrder_VBELV, SO.SalesItem_POSNV,
    SO.InvoiceUoM_MEINS, SO.InvoiceCurrency_WAERS,
    SO.MaterialNumber_MATNR,
    SO.MaterialText_MAKTX,
    SO.DeliveredUoM_MEINS,
    SO.ProductHierarchy_PRODH,
    SO.Language_SPRAS,
    sum(SO.InvoiceQty_RFMNG) AS BilledQty,
    sum(SO.InvoiceValue_RFWRT) AS InvoicePrice,
    sum(SO.DeliveredQty_RFMNG) AS DeliveredQty,
    ( sum(SO.DeliveredQty_RFMNG) - sum(SO.InvoiceQty_RFMNG) ) AS DeliveredPendingBilling
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesStatus_Items` AS SO
  GROUP BY SO.Client_MANDT, SO.SalesOrder_VBELV, SO.SalesItem_POSNV,
    Currency_WAERK, DeliveredUoM_MEINS, InvoiceUoM_MEINS, InvoiceCurrency_WAERS,
    SalesUnit_VRKME, MaterialNumber_MATNR, MaterialText_MAKTX,
    ProductHierarchy_PRODH, Language_SPRAS
)
SELECT SO.Client_MANDT,
  SO.SalesOrder_VBELV,
  SO.SalesItem_POSNV,
  SO.MaterialNumber_MATNR,
  SO.MaterialText_MAKTX,
  vbap.SalesUnit_VRKME,
  vbap.Currency_WAERK,
  SO.DeliveredQty,
  SO.DeliveredUoM_MEINS,
  SO.DeliveredPendingBilling,
  vbap.SalesOrganization_VKORG,
  vbap.Plant_WERKS,
  vbap.StorageLocation_LGORT,
  SO.Language_SPRAS,
  sum(vbap.CumulativeOrderQuantity_KWMENG) AS SalesQty,
  sum(vbap.NetPrice_NETWR) AS NetPrice, ( sum(vbap.CumulativeOrderQuantity_KWMENG) - SO.DeliveredQty ) AS PendingDelivery
FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesOrders` AS vbap
LEFT OUTER JOIN SO
  ON SO.Client_MANDT = vbap.Client_MANDT
    AND SO.MaterialNumber_MATNR = vbap.MaterialNumber_MATNR
    AND SO.InvoiceCurrency_WAERS = vbap.Currency_WAERK
GROUP BY SO.Client_MANDT, SO.SalesOrder_VBELV, SO.SalesItem_POSNV,
  vbap.SalesUnit_VRKME, vbap.Currency_WAERK, SO.MaterialNumber_MATNR, SO.MaterialText_MAKTX,
  SO.DeliveredUoM_MEINS, SO.DeliveredQty, SO.DeliveredUoM_MEINS,
  SO.DeliveredPendingBilling, vbap.SalesOrganization_VKORG, vbap.Plant_WERKS,
  vbap.StorageLocation_LGORT, SO.Language_SPRAS
