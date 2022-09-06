SELECT 
  mandt,
  matnr, 
  werks, 
  CASE shkzg
            WHEN 'H' THEN
                SUM(MENGE * -1)
            WHEN 'S' THEN
                SUM(menge)
  END as Quantity
#TODO(kuchhala): Rolling week   
## Richemont: Add batch level
#boutique = plant 
 FROM `kittycorn-dev-infy.SAP_CDC_PROCESSED_ECC.mseg` 
 WHERE INSMK = 'F' -- Unrestricted Use
 -- ## CORTEX-CUSTOMER: Implement movement types that do not add to unrestricted stock
 AND bwart not in ( '322', '323', '349', '344', '341', '321', '324', '350', '343', '342')
 group by  mandt, matnr, werks, shkzg