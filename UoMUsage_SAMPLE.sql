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
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.UoMUsage_SAMPLE`
OPTIONS(
  description = "Sample view using the UoMConversionUtil view"
)
AS
SELECT
  Salesfull.MaterialNumber_MATNR,
  Salesfull.MaterialText_MAKTX,
  Salesfull.DeliveredUoM_MEINS,
  Salesfull.DeliveredQty,
  conv.val_out AS conv_factor,
  if( Salesfull.DeliveredUoM_MEINS IN ('G', 'LB', 'KG'), 'LB', Salesfull.DeliveredUoM_MEINS ) AS to_conv,
  ( Salesfull.DeliveredQty * conv.val_out ) AS converted
FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.SalesFulfillment` AS Salesfull
INNER JOIN `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.UoMConversionUtil` AS conv
  ON Salesfull.Client_MANDT = conv.mandt AND Salesfull.DeliveredUoM_MEINS = conv.unit_from
    AND conv.unit_to = if( Salesfull.DeliveredUoM_MEINS IN ('G', 'LB', 'KG'), 'LB', Salesfull.DeliveredUoM_MEINS )
WHERE Salesfull.DeliveredUoM_MEINS IN ('G', 'KG')
