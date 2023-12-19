CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVGLAccountMapping`()
BEGIN
  --This procedure generates table having glaccounts mapped to fsv hierarchy nodes.
  DECLARE parent_node ARRAY <STRING>;
  DECLARE glaccount_array ARRAY <STRING>;
  DECLARE i INT64 DEFAULT 0;
  DECLARE len INT64 DEFAULT NULL;
  DECLARE input_parent STRING;

  CREATE OR REPLACE FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FindParent`(
    input_parent STRING)
  RETURNS STRING
  AS ((
      SELECT DISTINCT parent
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
      WHERE node = input_parent
  ));

  CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts`(
    mandt STRING,
    chartofaccounts STRING,
    hiername STRING,
    --noqa: disable=L008
    {% if sql_flavour == 's4' -%} hierarchyversion STRING,
    {% endif -%}
    parent STRING,
    node STRING,
    {% if sql_flavour == 'ecc' -%} ergsl STRING,
    {% else -%}
    nodevalue STRING,
    {% endif -%}
    glaccount STRING,
    --noqa: enable=all
    level STRING,
    isleafnode BOOL
  );

  --inserting leaf nodes with GL Account mapping
  INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts`
  --noqa: disable=L008
  (mandt, chartofaccounts, hiername,
    {% if sql_flavour == 's4' -%} hierarchyversion,
    {% endif -%}
    parent, node,
    {% if sql_flavour == 'ecc' -%} ergsl,
    {% else -%}
    nodevalue,
    {% endif -%}
    glaccount, level, isleafnode)
  SELECT
    mandt,
    chartofaccounts,
    hiername,
    {% if sql_flavour == 's4' -%} hierarchyversion,
    {% endif -%}
    parent,
    node,
    {% if sql_flavour == 'ecc' -%} ergsl,
    node,
    {% else -%}
    nodevalue,
    nodevalue,
    {% endif -%}
    level,
    isleafnode
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
  WHERE
    isleafnode IS TRUE
    AND node = {% if sql_flavour == 'ecc' -%} ergsl --noqa: disable=L006
    {% else -%}
    CONCAT('1',nodevalue)
    {% endif -%}; --noqa: enable=all

  --noqa: disable=L003
  SET parent_node = ARRAY(
    SELECT DISTINCT parent
    FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts`);
  SET len = ARRAY_LENGTH(parent_node);
  --noqa: enable=all

  --recursively inserting GLAccounts for each non-leaf node
  WHILE i < len DO
  SET input_parent = parent_node[i];
  SET glaccount_array = ARRAY(
    SELECT DISTINCT glaccount FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts`
    WHERE parent = input_parent);

    --noqa: disable=L008
    WHILE (SELECT DISTINCT level
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`
      WHERE parent = `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FindParent`(input_parent)
      ) != {% if sql_flavour == 'ecc' -%} '01' {% else -%} '000001'  {% endif -%}
      DO
      INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_glaccounts`
      (mandt, chartofaccounts, hiername,
        {% if sql_flavour == 's4' -%}
        hierarchyversion,
        {% endif -%}
        parent, node,
        {% if sql_flavour == 'ecc' -%} ergsl,
        {% else -%}
        nodevalue,
        {% endif -%}
        glaccount, level, isleafnode)
      SELECT
        mandt,
        chartofaccounts,
        hiername,
        {% if sql_flavour == 's4' -%} hierarchyversion,
        {% endif -%}
        parent,
        node,
        {% if sql_flavour == 'ecc' -%} ergsl,
        {% else -%}
        nodevalue,
        {% endif -%}
        glaccount, --noqa: enable=all
        level,
        isleafnode
      FROM
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fsv_flattened`,
        UNNEST (glaccount_array) AS glaccount
        WHERE node = input_parent;
        SET input_parent = `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FindParent`(
          input_parent);
    END WHILE; -- noqa: L003
    SET i = i + 1;
  END WHILE;

  DROP FUNCTION `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FindParent`;

  END;

  CALL `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FSVGLAccountMapping`();
