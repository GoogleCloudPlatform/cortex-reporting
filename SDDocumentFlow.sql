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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SDDocumentFlow`
OPTIONS(
  description = "Sales Document Flow (Sales Doc, Delivery, Billing)"
)
AS
SELECT
  SO.mandt AS Client_MANDT,
  SO.VBELV AS SalesOrder_VBELV,
  SO.POSNV AS SalesItem_POSNV,
  Deliveries.VBELV AS DeliveryNumber_VBELV,
  Deliveries.POSNV AS DeliveryItem_POSNV,
  Deliveries.VBELN AS InvoiceNumber_VBELN,
  Deliveries.POSNN AS InvoiceItem_POSNN,
  SO.RFMNG AS DeliveredQty_RFMNG,
  SO.MEINS AS DeliveredUoM_MEINS,
  Deliveries.RFMNG AS InvoiceQty_RFMNG,
  Deliveries.MEINS AS InvoiceUoM_MEINS,
  Deliveries.RFWRT AS InvoiceValue_RFWRT,
  Deliveries.WAERS AS InvoiceCurrency_WAERS
FROM `{{ project_id_src }}.{{ dataset_cdc_processed }}.vbfa` AS SO
LEFT OUTER JOIN `{{ project_id_src }}.{{ dataset_cdc_processed }}.vbfa` AS Deliveries
  ON SO.VBELN = Deliveries.VBELV AND SO.mandt = Deliveries.mandt
    AND SO.POSNN = Deliveries.POSNV
WHERE SO.vbtyp_V = 'C'
  AND SO.vbtyp_n IN ('J', 'T')
  AND Deliveries.vbtyp_n IN ('M')
ORDER BY SO.VBELV
