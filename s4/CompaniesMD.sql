SELECT
  T001.MANDT AS Client_MANDT,
  T001.WAERS AS CurrencyCode_WAERS,
  T001.BUKRS AS CompanyCode_BUKRS,
  T001.BUTXT AS CompanyText_BUTXT,
  T001.ORT01 AS CityName_ORT01,
  T001.LAND1 AS Country_LAND1,
  T001.SPRAS AS Language_SPRAS,
  T001.KTOPL AS ChartOfAccounts_KTOPL,
  T001.PERIV AS FiscalyearVariant_PERIV,
  T001.RCOMP AS Company_RCOMP,
  T001.KKBER AS CreditControlArea_KKBER,
  T001.KTOP2 AS CountryChartofAccounts_KTOP2,
  T001.FIKRS AS FundsManagement_FIKRS

FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.t001` AS T001
