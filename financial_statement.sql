CREATE OR REPLACE TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement` (
  client STRING,
  companycode STRING,
  businessarea STRING,
  ledger STRING,
  profitcenter STRING,
  costcenter STRING,
  glaccount STRING,
  fiscalyear STRING,
  fiscalperiod STRING,
  fiscalquarter INT64,
  --noqa: disable=L008
  {% if sql_flavour == 'ecc' -%}
  balancesheetaccountindicator STRING,
  placcountindicator STRING,
  {% else %}
  balancesheetandplaccountindicator STRING,
  {% endif -%}
  --noqa: enable=all
  amount NUMERIC,
  currency STRING,
  companytext STRING
);

CREATE OR REPLACE PROCEDURE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.FinancialStatement`(
  input_startdate DATE, input_enddate DATE)
BEGIN
  --This procedure creates table having transaction data at fiscal year, period level
  --including copying missing records from one period to another.
  DECLARE sequence_length INT64 DEFAULT NULL;
  DECLARE fiscal_iteration INT64 DEFAULT 2;

  ALTER TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
  ADD COLUMN sequence INT;
  {% if sql_flavour == 'ecc' -%}
    CREATE OR REPLACE TEMP TABLE AccountingDocuments AS (
      SELECT
        bkpf.MANDT,
        bkpf.BUKRS,
        bkpf.RLDNR,
        bseg.GSBER,
        bseg.KOSTL,
        bseg.HKONT,
        bseg.PRCTR,
        FiscalDateDimension.FiscalYear,
        FiscalDateDimension.FiscalPeriod,
        MAX(FiscalDateDimension.FiscalQuarter) AS FiscalQuarter,
        MAX(bseg.XBILK) AS XBILK,
        MAX(bseg.GVTYP) AS GVTYP,
        MAX(t001.PERIV) AS PERIV,
        MAX(t001.BUTXT) AS BUTXT,
        MAX(bkpf.WAERS) AS WAERS,
        SUM(COALESCE(
          IF(bseg.SHKZG = 'S',
            bseg.DMBTR,
            IF(bseg.SHKZG = 'H', bseg.DMBTR * -1, bseg.DMBTR)) * currency_decimal.CURRFIX,
          IF(bseg.SHKZG = 'S',
            bseg.DMBTR,
            IF(bseg.SHKZG = 'H', bseg.DMBTR * -1, bseg.DMBTR))
        )) AS DMBTR
      FROM
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.bseg` AS bseg
      INNER JOIN
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.bkpf` AS bkpf
        ON
          bkpf.MANDT = bseg.MANDT
          AND bkpf.BUKRS = bseg.BUKRS
          AND bkpf.GJAHR = bseg.GJAHR
          AND bkpf.BELNR = bseg.BELNR
      LEFT JOIN
        `{{ project_id_src }}.{{ dataset_cdc_processed }}.t001` AS t001
        ON
          bseg.MANDT = t001.MANDT
          AND bseg.BUKRS = t001.BUKRS
      LEFT JOIN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
        ON bkpf.WAERS = currency_decimal.CURRKEY
      LEFT JOIN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension
        ON
          bseg.MANDT = FiscalDateDimension.MANDT
          AND t001.PERIV = FiscalDateDimension.PERIV
          AND bkpf.BUDAT = FiscalDateDimension.DATE
      WHERE
        bseg.MANDT = '{{ mandt }}'
        --Ignoring the reversal documents
        AND bkpf.XREVERSAL IS NULL
      GROUP BY
        bkpf.MANDT, bkpf.BUKRS, bkpf.RLDNR, bseg.GSBER, bseg.KOSTL, bseg.HKONT, bseg.PRCTR,
        FiscalDateDimension.FiscalYear, FiscalDateDimension.FiscalPeriod
    );

    CREATE OR REPLACE TEMP TABLE FiscalDimension AS (
      SELECT DISTINCT
        AccountingDocuments.MANDT,
        AccountingDocuments.BUKRS,
        FiscalDateDimension.FiscalYear,
        FiscalDateDimension.FiscalPeriod,
        FiscalDateDimension.FiscalQuarter,
        DENSE_RANK() OVER (
          ORDER BY AccountingDocuments.BUKRS ASC,
            FiscalDateDimension.FiscalYear ASC,
            FiscalDateDimension.FiscalPeriod ASC) AS sequence
      FROM (
          SELECT DISTINCT MANDT, BUKRS, PERIV
          FROM AccountingDocuments) AS AccountingDocuments
      LEFT JOIN
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension
        ON
          AccountingDocuments.MANDT = FiscalDateDimension.MANDT
          AND AccountingDocuments.PERIV = FiscalDateDimension.PERIV
      WHERE
        FiscalDateDimension.DATE
        BETWEEN
          input_startdate
        AND
          input_enddate -- noqa: L027
    );

    DELETE FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
    WHERE
      fiscalyear IN (SELECT FiscalYear FROM FiscalDimension)
      AND fiscalperiod IN (SELECT FiscalPeriod FROM FiscalDimension);

    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
    (client, companycode, businessarea, ledger, profitcenter, costcenter, glaccount,
     fiscalyear, fiscalperiod, fiscalquarter, balancesheetaccountindicator, placcountindicator,
     amount, currency, companytext, sequence)
    SELECT
      FiscalDimension.MANDT,
      FiscalDimension.BUKRS,
      AccountingDocuments.GSBER,
      AccountingDocuments.RLDNR,
      AccountingDocuments.PRCTR,
      AccountingDocuments.KOSTL,
      AccountingDocuments.HKONT,
      FiscalDimension.FiscalYear,
      FiscalDimension.FiscalPeriod,
      FiscalDimension.FiscalQuarter,
      AccountingDocuments.XBILK,
      AccountingDocuments.GVTYP,
      COALESCE(AccountingDocuments.DMBTR, 0),
      AccountingDocuments.WAERS,
      AccountingDocuments.BUTXT,
      FiscalDimension.sequence
    FROM FiscalDimension
    LEFT JOIN AccountingDocuments
      ON
        FiscalDimension.MANDT = AccountingDocuments.MANDT
        AND FiscalDimension.BUKRS = AccountingDocuments.BUKRS
        AND FiscalDimension.FiscalYear = AccountingDocuments.FiscalYear
        AND FiscalDimension.FiscalPeriod = AccountingDocuments.FiscalPeriod;

  {% else -%}
  CREATE OR REPLACE TEMP TABLE AccountingDocuments AS (
    SELECT
      acdoca.RCLNT,
      acdoca.RBUKRS,
      acdoca.RLDNR,
      acdoca.RBUSA,
      acdoca.RCNTR,
      acdoca.RACCT,
      acdoca.PRCTR,
      FiscalDateDimension.FiscalYear,
      FiscalDateDimension.FiscalPeriod,
      MAX(FiscalDateDimension.FiscalQuarter) AS FiscalQuarter,
      MAX(acdoca.GLACCOUNT_TYPE) AS GLACCOUNT_TYPE,
      MAX(t001.PERIV) AS PERIV,
      MAX(t001.BUTXT) AS BUTXT,
      MAX(acdoca.RHCUR) AS RHCUR,
      SUM(COALESCE(acdoca.HSL * currency_decimal.CURRFIX, acdoca.HSL)) AS HSL
    FROM
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.acdoca` AS acdoca
    LEFT JOIN
      `{{ project_id_src }}.{{ dataset_cdc_processed }}.t001` AS t001
      ON acdoca.RCLNT = t001.MANDT AND acdoca.RBUKRS = t001.BUKRS
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_decimal` AS currency_decimal
      ON acdoca.RHCUR = currency_decimal.CURRKEY
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension
      ON
        acdoca.RCLNT = FiscalDateDimension.MANDT
        AND t001.PERIV = FiscalDateDimension.PERIV
        AND acdoca.BUDAT = FiscalDateDimension.DATE
    WHERE
      acdoca.RCLNT = '{{ mandt }}'
      --Ignoring the reversal documents
      AND acdoca.XTRUEREV IS NULL
      --- Ensuring we only get records for PL and BalanceSheet
      AND acdoca.GLACCOUNT_TYPE IN ('X','P','N')
    GROUP BY RCLNT,RBUKRS,RLDNR,RBUSA,RCNTR,RACCT,PRCTR,FiscalYear,FiscalPeriod
  );

  CREATE OR REPLACE TEMP TABLE FiscalDimension
  AS (
    SELECT DISTINCT
      AccountingDocuments.RCLNT,
      AccountingDocuments.RBUKRS,
      FiscalDateDimension.FiscalYear,
      FiscalDateDimension.FiscalPeriod,
      FiscalDateDimension.FiscalQuarter,
      DENSE_RANK() OVER (
        ORDER BY AccountingDocuments.RBUKRS ASC,
          FiscalDateDimension.FiscalYear ASC,
          FiscalDateDimension.FiscalPeriod ASC) AS sequence
    FROM (
        SELECT DISTINCT
          RCLNT,
          RBUKRS,
          PERIV
        FROM AccountingDocuments
    ) AS AccountingDocuments
    LEFT JOIN
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.fiscal_date_dim` AS FiscalDateDimension
      ON
        AccountingDocuments.RCLNT = FiscalDateDimension.MANDT
        AND AccountingDocuments.PERIV = FiscalDateDimension.PERIV
    WHERE
      FiscalDateDimension.DATE
      BETWEEN
        input_startdate
      AND
        input_enddate -- noqa: L027
  );

  DELETE FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
  WHERE
    fiscalyear IN (SELECT FiscalYear FROM FiscalDimension)
    AND fiscalperiod IN (SELECT FiscalPeriod FROM FiscalDimension);

  INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
  (client, companycode, businessarea, ledger, profitcenter, costcenter, glaccount,
   fiscalyear, fiscalperiod, fiscalquarter, balancesheetandplaccountindicator, amount, currency,
   companytext, sequence)
  SELECT
    FiscalDimension.RCLNT,
    FiscalDimension.RBUKRS,
    AccountingDocuments.RBUSA,
    AccountingDocuments.RLDNR,
    AccountingDocuments.PRCTR,
    AccountingDocuments.RCNTR,
    AccountingDocuments.RACCT,
    FiscalDimension.FiscalYear,
    FiscalDimension.FiscalPeriod,
    FiscalDimension.FiscalQuarter,
    AccountingDocuments.GLACCOUNT_TYPE,
    COALESCE(AccountingDocuments.HSL, 0),
    AccountingDocuments.RHCUR,
    AccountingDocuments.BUTXT,
    FiscalDimension.sequence
  FROM FiscalDimension
  LEFT JOIN AccountingDocuments
    ON
      FiscalDimension.RCLNT = AccountingDocuments.RCLNT
      AND FiscalDimension.RBUKRS = AccountingDocuments.RBUKRS
      AND FiscalDimension.FiscalYear = AccountingDocuments.FiscalYear
      AND FiscalDimension.FiscalPeriod = AccountingDocuments.FiscalPeriod;

  {% endif -%}
  SET sequence_length = (
      SELECT MAX(sequence)
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`);
  --Copying records to current period
  --in case they exist in previous period but not in current period
  WHILE (fiscal_iteration<=sequence_length) DO
    INSERT INTO `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
    (client, companycode, businessarea, ledger, profitcenter, costcenter, glaccount,
    fiscalyear, fiscalperiod, fiscalquarter,
    --noqa: disable=L008
    {% if sql_flavour == 'ecc' -%} balancesheetaccountindicator,
    placcountindicator,
    {% else -%}
    balancesheetandplaccountindicator,
    {% endif -%}
    amount, currency, companytext, sequence)
    --noqa: enable=all
    WITH
      PreviousPeriod AS (
        SELECT * EXCEPT(amount),
          CONCAT(glaccount, COALESCE(profitcenter,''), companycode,
            COALESCE(businessarea,''), COALESCE(costcenter,''),
            COALESCE(ledger,'')) AS uniquecombination
      FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
      WHERE sequence = fiscal_iteration-1
      ),
      CurrentPeriod AS (
        SELECT fiscalyear, fiscalperiod, fiscalquarter, sequence,
          CONCAT(glaccount, COALESCE(profitcenter,''), companycode,
            COALESCE(businessarea,''), COALESCE(costcenter,''),
            COALESCE(ledger,'')) AS uniquecombination
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
        WHERE sequence = fiscal_iteration
      )
      SELECT
        client,
        companycode,
        businessarea,
        ledger,
        profitcenter,
        costcenter,
        glaccount,
        (SELECT MAX(fiscalyear) FROM CurrentPeriod),
        (SELECT MAX(fiscalperiod) FROM CurrentPeriod),
        (SELECT MAX(fiscalquarter) FROM CurrentPeriod),
        --noqa: disable=L008
        {% if sql_flavour == 'ecc' -%} balancesheetaccountindicator,
        placcountindicator,
        {% else -%}
        balancesheetandplaccountindicator,
        {% endif -%}
        0 AS amount,
        --noqa: enable=all
        currency,
        companytext,
        (SELECT MAX(sequence) FROM CurrentPeriod)
      FROM PreviousPeriod
      WHERE uniquecombination NOT IN (SELECT DISTINCT uniquecombination FROM CurrentPeriod);
      SET fiscal_iteration = fiscal_iteration + 1;
  END WHILE;
  DELETE FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
  WHERE glaccount IS NULL;
  ALTER TABLE `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.financial_statement`
  DROP COLUMN sequence;
END;
