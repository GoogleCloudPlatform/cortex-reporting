CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCenterMapping`()
BEGIN
  --This procedure generates table having profit center mapped to profit center hierarchy nodes.
  DECLARE rownum INT64 DEFAULT 1;
  DECLARE maximum_len INT64 DEFAULT NULL;

  CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profit_centers`(
    mandt STRING,
    setclass STRING,
    subclass STRING,
    hiername STRING,
    parent STRING,
    node STRING,
    profitcenter STRING,
    level INT64,
    isleafnode BOOL,
    rownumber INT64
  );

  --inserting leaf nodes with profit center mapping
  INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profit_centers`
  (mandt, setclass, subclass, hiername, parent, node, profitcenter, level, isleafnode, rownumber)
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
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profitcenter_flattened`
  WHERE isleafnode IS TRUE
    AND level = 0;

  SET maximum_len = (
    SELECT MAX(level) FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profitcenter_flattened`
  );

  --insert profit center mapping by parent node combination for each level
  WHILE maximum_len != 0 DO
    SET rownum = rownum+1;
    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profit_centers` --noqa: disable=L003
    (mandt, setclass, subclass, hiername, parent, node, profitcenter, level, isleafnode, rownumber)
    SELECT
      profitcentermapping.mandt,
      profitcentermapping.setclass,
      profitcentermapping.subclass,
      profitcentermapping.hiername,
      profitcenterflattened.parent,
      profitcenterflattened.node,
      profitcentermapping.profitcenter,
      profitcenterflattened.level,
      profitcenterflattened.isleafnode,
      rownum -- noqa: disable=L027
    FROM
      (SELECT *
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profit_centers`
        WHERE level != 1 AND rownumber = rownum - 1) AS profitcentermapping
    INNER JOIN
      (SELECT * -- noqa: disable=L042
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profitcenter_flattened`
        WHERE level != 0 OR isleafnode
      ) AS profitcenterflattened
      ON profitcentermapping.mandt = profitcenterflattened.mandt
        AND profitcentermapping.setclass = profitcenterflattened.setclass
        AND profitcentermapping.subclass = profitcenterflattened.subclass
        AND profitcentermapping.hiername = profitcenterflattened.hiername
        AND profitcentermapping.parent = profitcenterflattened.node;
    SET maximum_len = maximum_len - 1;
  END WHILE;
  --noqa: enable=all

  ALTER TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.profit_centers`
  DROP COLUMN rownumber;
END;

CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.ProfitCenterMapping`();
