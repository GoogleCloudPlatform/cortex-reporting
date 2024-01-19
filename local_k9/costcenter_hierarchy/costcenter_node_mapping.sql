CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterMapping`()
BEGIN
  --This procedure generates table having cost center mapped to cost center hierarchy nodes.
  DECLARE rownum INT64 DEFAULT 1;
  DECLARE maximum_len INT64 DEFAULT NULL;

  CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.cost_centers`(
    mandt STRING,
    setclass STRING,
    subclass STRING,
    hiername STRING,
    parent STRING,
    node STRING,
    costcenter STRING,
    level INT64,
    isleafnode BOOL,
    rownumber INT64
  );

  --inserting leaf nodes with cost center mapping
  INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.cost_centers`
  (mandt, setclass, subclass, hiername, parent, node, costcenter, level, isleafnode, rownumber)
  SELECT
    mandt,
    setclass,
    subclass,
    hiername,
    parent,
    node,
    node,
    level,
    isleafnode,
    rownum
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
  WHERE isleafnode IS TRUE
    AND level = 0;

  SET maximum_len = (
    SELECT MAX(level) FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
  );

  --insert cost center mapping by parent node combination for each level
  WHILE maximum_len != 0 DO
    SET rownum = rownum+1;
    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.cost_centers`  --noqa: disable=L003
    (mandt, setclass, subclass, hiername, parent, node, costcenter, level, isleafnode, rownumber)
    SELECT
      costcentermapping.mandt,
      costcentermapping.setclass,
      costcentermapping.subclass,
      costcentermapping.hiername,
      costcenterflattened.parent,
      costcenterflattened.node,
      costcentermapping.costcenter,
      costcenterflattened.level,
      costcenterflattened.isleafnode,
      rownum -- noqa: disable=L027
    FROM
      (SELECT *
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.cost_centers`
        WHERE level != 1 AND rownumber = rownum - 1) AS costcentermapping
    INNER JOIN
      (SELECT * -- noqa: disable=L042
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.costcenter_flattened`
        WHERE level != 0 OR isleafnode
      ) AS costcenterflattened
      ON costcentermapping.mandt = costcenterflattened.mandt
        AND costcentermapping.setclass = costcenterflattened.setclass
        AND costcentermapping.subclass = costcenterflattened.subclass
        AND costcentermapping.hiername = costcenterflattened.hiername
        AND costcentermapping.parent = costcenterflattened.node;
    SET maximum_len = maximum_len - 1;
  END WHILE;
  --noqa: enable=all

  ALTER TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.cost_centers`
  DROP COLUMN rownumber;
END;

CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CostCenterMapping`();
