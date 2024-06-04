#-- Copyright 2024
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

#-- EXPERIMENTAL - VIEW/FIELD NAMES/TYPES SUBJECT TO CHANGE

SELECT
    mandt AS Client_MANDT,
    setclass AS HierarchyClass_SETCLASS,
    subclass AS HierarchySubClass_SUBCLASS,
    hiername AS HierarchyType_HIERBASE,
    parent AS ParentNode_PRED,
    CCParentText.SetName_SETNAME AS ParentNodeText_SETNAME,
    node AS CostCenterNode,
    COALESCE(CCNodeText.SetName_SETNAME, CCText.Description_LTEXT) AS CostCenterNodeText,
    level AS Level_HLEVEL,
    costcenter AS CostCenter_KOSTL
FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.cost_centers` AS CostCenters
LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterHierarchiesMD` AS CCParentText
ON
    CostCenters.mandt = CCParentText.Client_MANDT
    AND CostCenters.setclass = CCParentText.SetClass_SETCLASS
    AND CostCenters.subclass = CCParentText.OrganizationalUnit_SUBCLASS
    AND CostCenters.parent = CAST(CCParentText.NodeNumber_SUCC AS STRING)
    AND CostCenters.hiername = CCParentText.SetName_HIERBASE
LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterHierarchiesMD` AS CCNodeText
ON
    CostCenters.mandt = CCNodeText.Client_MANDT
    AND CostCenters.setclass = CCNodeText.SetClass_SETCLASS
    AND CostCenters.subclass = CCNodeText.OrganizationalUnit_SUBCLASS
    AND CostCenters.node = CAST(CCNodeText.NodeNumber_SUCC AS STRING)
    AND CostCenters.hiername = CCParentText.SetName_HIERBASE
LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCentersMD` AS CCText
 ON
    CostCenters.mandt = CCText.Client_MANDT
    AND CostCenters.subclass = CCText.ControllingArea_KOKRS
    AND CostCenters.node = CCText.CostCenter_KOSTL
    -- # CORTEX-CUSTOMER Update to use a single language of your choice for Node texts
    AND CCText.Language_SPRAS = 'E'
    AND CCText.ValidTo_DATBI >= '9999-12-31'
    