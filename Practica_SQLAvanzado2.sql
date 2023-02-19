CREATE OR REPLACE TABLE keepcoding.ivr_summary AS(
WITH PRACTICA_P2 AS(
SELECT calls_phone_number AS phone_number
      ,calls_start_date AS start_date
      ,IF(TIMESTAMP_DIFF(calls_start_date, LAG(calls_start_date) OVER(ORDER BY calls_phone_number), HOUR) < 24
       AND calls_phone_number = LAG(calls_phone_number) OVER(ORDER BY calls_phone_number), 1, 0) AS repeated_phone_24H
      ,IF(TIMESTAMP_DIFF(LEAD(calls_start_date) OVER(ORDER BY calls_phone_number), calls_start_date, HOUR) < 24
       AND calls_phone_number = LEAD(calls_phone_number) OVER(ORDER BY calls_phone_number), 1, 0) AS cause_recall_phone_24H
  FROM keepcoding.ivr_detail
  GROUP BY calls_phone_number, calls_start_date, calls_ivr_id, calls_phone_number
  ORDER BY calls_phone_number, calls_start_date ASC)


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
      ,IFNULL(NULLIF(d1.document_type, 'NULL'), 'DESCONOCIDO') AS document_type
      ,IFNULL(NULLIF(d1.document_identification, 'NULL'), 'DESCONOCIDO') AS document_identification
      ,IFNULL(NULLIF(d1.customer_phone, 'NULL'), 'DESCONOCIDO') AS customer_phone
      ,IFNULL(NULLIF(d1.billing_account_id, 'NULL'), 'DESCONOCIDO') AS billing_account_id
      ,MAX(IF(d2.module_name = 'AVERIA_MASIVA', 1, 0)) AS masiva_lg
      ,MAX(IF(d2.step_name = 'CUSTOMERINFOBYPHONE.TX' 
       AND d2.step_description_error = 'NULL', 1, 0)) AS info_by_phone_lg
      ,MAX(IF(d2.step_name = 'CUSTOMERINFOBYDNI.TX' 
       AND d2.step_description_error = 'NULL', 1, 0)) AS info_by_dni_lg
      ,PRACTICA_P2.repeated_phone_24H
      ,PRACTICA_P2.cause_recall_phone_24H
  FROM keepcoding.ivr_detail d1
    JOIN keepcoding.ivr_detail d2
    ON d1.calls_ivr_id = d2.calls_ivr_id
    JOIN PRACTICA_P2
    ON phone_number = d1.calls_phone_number AND start_date = d1.calls_start_date
    GROUP BY d1.calls_ivr_id, d1.calls_phone_number, d1.calls_ivr_result, d1.calls_vdn_label, d1.calls_start_date
            ,d1.calls_end_date, d1.calls_total_duration, d1.calls_customer_segment, d1.calls_ivr_language
            ,d1.calls_steps_module, d1.calls_module_aggregation, d1.document_type, d1.document_identification, d1.customer_phone
            ,d1.billing_account_id, d1.module_name, d1.step_name, d1.step_description_error, d1.calls_ivr_id
            ,d2.calls_start_date, d2.calls_phone_number, d2.calls_ivr_id, phone_number,PRACTICA_P2.repeated_phone_24H
            ,PRACTICA_P2.cause_recall_phone_24H
        QUALIFY ROW_NUMBER() OVER(PARTITION BY d1.calls_ivr_id ORDER BY d1.document_type NULLS LAST,
        d1.document_identification NULLS LAST, d1.customer_phone NULLS LAST, d1.billing_account_id NULLS LAST) = 1
      ORDER BY phone_number, start_date ASC)