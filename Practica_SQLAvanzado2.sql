CREATE OR REPLACE TABLE keepcoding.PRACTICA_P2 AS( --PRIMERA PARTE DE LA TABLA 2
SELECT d1.calls_ivr_id AS ivr_id
      ,d1.calls_phone_number AS phone_number
      ,d1.calls_ivr_result AS ivr_result
            , CASE WHEN d1.calls_vdn_label LIKE 'ATC%' THEN 'FRONT'
                   WHEN d1.calls_vdn_label LIKE 'TECH%' THEN 'TECH'
                   WHEN d1.calls_vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
              ELSE 'RESTO'
            END AS vdn_aggregation
      ,d1.calls_start_date AS start_date
      ,d1.calls_end_date AS end_date
      ,d1.calls_total_duration AS total_duration
      ,d1.calls_customer_segment AS customer_segment
      ,d1.calls_ivr_language AS ivr_language
      ,d1.calls_steps_module AS steps_module
      ,d1.calls_module_aggregation AS module_aggregation
      ,NULLIF(d1.document_type, 'NULL') AS document_type
      ,NULLIF(d1.document_identification, 'NULL') AS document_identification
      ,NULLIF(d1.customer_phone, 'NULL') AS customer_phone
      ,NULLIF(d1.billing_account_id, 'NULL') AS billing_account_id
      ,MAX(IF(d2.module_name = 'AVERIA_MASIVA', 1, 0)) AS masiva_lg
      ,MAX(IF(d2.step_name = 'CUSTOMERINFOBYPHONE.TX' 
       AND d2.step_description_error = 'NULL', 1, 0)) AS info_by_phone_lg
      ,MAX(IF(d2.step_name = 'CUSTOMERINFOBYDNI.TX' 
       AND d2.step_description_error = 'NULL', 1, 0)) AS info_by_dni_lg
  FROM keepcoding.ivr_detail d1
    JOIN keepcoding.ivr_detail d2
    ON d1.calls_ivr_id = d2.calls_ivr_id
    GROUP BY d1.calls_ivr_id, d1.calls_phone_number, d1.calls_ivr_result, d1.calls_vdn_label, d1.calls_start_date
            ,d1.calls_end_date, d1.calls_total_duration, d1.calls_customer_segment, d1.calls_ivr_language
            ,d1.calls_steps_module, d1.calls_module_aggregation, d1.document_type, d1.document_identification, d1.customer_phone
            ,d1.billing_account_id, d1.module_name, d1.step_name, d1.step_description_error, d1.calls_ivr_id
            ,d2.calls_start_date, d2.calls_phone_number, d2.calls_ivr_id
        QUALIFY ROW_NUMBER() OVER(PARTITION BY d1.calls_ivr_id ORDER BY d1.document_type NULLS LAST,
        d1.document_identification NULLS LAST, d1.customer_phone NULLS LAST, d1.billing_account_id NULLS LAST) = 1
      ORDER BY d1.calls_phone_number, d1.calls_start_date ASC, d2.calls_phone_number, d2.calls_start_date ASC)
;
CREATE OR REPLACE TABLE keepcoding.ivr_summary AS( -- SEGUNDA PARTE DE LA TABLA 2
SELECT ivr_id
      ,phone_number
      ,ivr_result
      ,vdn_aggregation
      ,start_date
      ,end_date
      ,total_duration
      ,customer_segment
      ,ivr_language
      ,steps_module
      ,module_aggregation
      ,IFNULL(document_type, 'DESCONOCIDO') AS document_type
      ,IFNULL(document_identification, 'DESCONOCIDO') AS document_identification
      ,IFNULL(customer_phone, 'DESCONOCIDO') AS customer_phone
      ,IFNULL(billing_account_id, 'DESCONOCIDO') AS billing_account_id
      ,masiva_lg
      ,info_by_phone_lg
      ,info_by_dni_lg
      ,IF(TIMESTAMP_DIFF(start_date, LAG(start_date) OVER(ORDER BY phone_number), HOUR) < 24
       AND phone_number = LAG(phone_number) OVER(ORDER BY phone_number), 1, 0) AS repeated_phone_24H
      ,IF(TIMESTAMP_DIFF(LEAD(start_date) OVER(ORDER BY phone_number), start_date, HOUR) < 24
       AND phone_number = LEAD(phone_number) OVER(ORDER BY phone_number), 1, 0) AS cause_recall_phone_24H

  FROM keepcoding.PRACTICA_P2
  GROUP BY ivr_id, phone_number, ivr_result, vdn_aggregation, start_date, end_date
          ,total_duration, customer_segment, ivr_language, steps_module, module_aggregation, document_type
          ,document_identification, customer_phone, billing_account_id, masiva_lg, info_by_phone_lg, info_by_dni_lg
  ORDER BY phone_number, start_date ASC)
