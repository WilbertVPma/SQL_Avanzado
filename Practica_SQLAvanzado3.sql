DELIMITER $$
CREATE TRIGGER NO NULLS
BEFORE INSERT ON keepcoding.ivr_summary
FOR EACH ROW 
BEGIN
    IF NEW.ivr_id = 'NULL' --COLUMNA ivr_id
      THEN SET NEW.age = "-999999";
    END IF; 
    IF NEW.phone_number = 'NULL' -- COLUMNA phone_number
      THEN SET NEW.phone_number = "-999999";
    END IF;
    IF NEW.ivr_result = 'NULL' -- COLUMNA ivr_result
      THEN SET NEW.ivr_result = "-999999";
    END IF;
    IF NEW.vdn_aggregation = 'NULL' -- COLUMNA vdn_aggregation
      THEN SET NEW.vdn_aggregation = "-999999";
    END IF;
    IF NEW.start_date = 'NULL' -- COLUMNA start_date
      THEN SET NEW.start_date = "-999999";
    END IF;
    IF NEW.end_date = 'NULL' -- COLUMNA end_date
      THEN SET NEW.end_date = "-999999";
    END IF;
    IF NEW.total_duration = 'NULL'-- COLUMNA total_duration
      THEN SET NEW.total_duration = "-999999";
    END IF;
    IF NEW.customer_segment = 'NULL' -- COLUMNA customer_segment
      THEN SET NEW.customer_segment = "-999999";
    END IF;
    IF NEW.ivr_language = 'NULL' -- COLUMNA ivr_language
      THEN SET NEW.ivr_language = "-999999";
    END IF;
    IF NEW.steps_module = 'NULL' -- COLUMNA steps_module
      THEN SET NEW.steps_module = "-999999";
    END IF;
    IF NEW.module_aggregation = 'NULL' -- COLUMNA module_aggregation
      THEN SET NEW.smodule_aggregation = "-999999";
    END IF;
    IF NEW.document_type = 'NULL' -- COLUMNA document_type
      THEN SET NEW.document_type = "-999999";
    END IF;
    IF NEW.document_identification = 'NULL' -- COLUMNA document_identification
      THEN SET NEW.document_identification = "-999999";
    END IF;
    IF NEW.customer_phone = 'NULL' -- COLUMNA customer_phone
      THEN SET NEW.customer_phone = "-999999";
    END IF;
    IF NEW.billing_account_id = 'NULL' -- COLUMNA billing_account_id
      THEN SET NEW.billing_account_id = "-999999";
    END IF;
END; $$