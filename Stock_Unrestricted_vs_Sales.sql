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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Stock_Unrestricted_vs_Sales`
OPTIONS(
  description = "Non-valuated unrestricted stock levels versus sales orders and deliveries"
)
AS
WITH stock AS (
  SELECT
    Client_MANDT,
    MaterialNumber_MATNR,
    MaterialText_MAKTX,
    Plant_WERKS,
    Plant_Name,
    BaseUnitOfMeasure_MEINS,
    ProductHierarchy_PRDHA,
    Plant_Region,
    Plant_Country,
    StorageLocation_LGORT,
    Language_SPRAS,
    SUM(ValuatedUnrestrictedUseStock) AS UnrestrictedStock
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.Stock_NonValuated`
  GROUP BY Client_MANDT, MaterialNumber_MATNR, MaterialText_MAKTX,
    Plant_WERKS, Plant_Name, BaseUnitOfMeasure_MEINS, ProductHierarchy_PRDHA,
    Plant_Region, Plant_Country, StorageLocation_LGORT, Language_SPRAS
),

sales AS (
  SELECT Client_MANDT,
    MaterialNumber_MATNR,
    SalesUnit_VRKME,
    DeliveredUoM_MEINS,
    Plant_WERKS,
    StorageLocation_LGORT, Language_SPRAS,
    SUM(SalesQty) AS SalesQty,
    SUM(DeliveredQty) AS DeliveredQty,
    SUM(PendingDelivery) AS PendingDelivery
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesFulfillment_perOrder`
  GROUP BY Client_MANDT, MaterialNumber_MATNR, SalesUnit_VRKME, DeliveredUoM_MEINS,
    Plant_WERKS, StorageLocation_LGORT, Language_SPRAS
)

SELECT stock.*,
  sales.SalesQty,
  sales.DeliveredQty,
  sales.DeliveredUoM_MEINS,
  sales.PendingDelivery
FROM sales
LEFT OUTER JOIN stock
  ON stock.Client_MANDT = sales.Client_MANDT
    AND stock.MaterialNumber_MATNR = sales.MaterialNumber_MATNR
    AND stock.Plant_WERKS = sales.Plant_WERKS
    AND stock.StorageLocation_LGORT = sales.StorageLocation_LGORT
    AND stock.Language_SPRAS = sales.Language_SPRAS
    AND stock.BaseUnitOfMeasure_MEINS = sales.DeliveredUoM_MEINS
WHERE stock.Language_SPRAS = 'E'
