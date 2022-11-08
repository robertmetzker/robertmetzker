
  create or replace  view DEV_EDW.STAGING.DSV_MEDICAL_INVOICE_PROCEDURE  as (
    

---- SRC LAYER ----
WITH
SRC_MD as ( SELECT *     from     STAGING.DST_MEDICAL_INVOICE_PROCEDURE ),
//SRC_MD as ( SELECT *     from     DST_MEDICAL_INVOICE_PROCEDURE) ,

---- LOGIC LAYER ----

LOGIC_MD as ( SELECT 
		  INVOICE_NUMBER                                     AS                                     INVOICE_NUMBER 
		, RECEIPT_DATE                                       AS                                       RECEIPT_DATE 
		, SERVICE_FROM                                       AS                                       SERVICE_FROM 
		, SERVICE_TO                                         AS                                         SERVICE_TO 
		, BATCH_NUMBER                                       AS                                       BATCH_NUMBER 
		, PRO_SEQUENCE_NUMBER                                AS                                PRO_SEQUENCE_NUMBER 
		, PROCEDURE_CODE                                     AS                                     PROCEDURE_CODE 
		, PROCEDURE_DATE                                     AS                                     PROCEDURE_DATE 
		, ICD_VERSION                                        AS                                        ICD_VERSION 
		from SRC_MD
            )

---- RENAME LAYER ----
,

RENAME_MD as ( SELECT 
		  INVOICE_NUMBER                                     as                             MEDICAL_INVOICE_NUMBER
		, RECEIPT_DATE                                       as                              BWC_BILL_RECEIPT_DATE
		, SERVICE_FROM                                       as                          INVOICE_SERVICE_FROM_DATE
		, SERVICE_TO                                         as                            INVOICE_SERVICE_TO_DATE
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER
		, PRO_SEQUENCE_NUMBER                                as                          PROCEDURE_SEQUENCE_NUMBER
		, PROCEDURE_CODE                                     as                                     PROCEDURE_CODE
		, PROCEDURE_DATE                                     as                                 ICD_PROCEDURE_DATE
		, ICD_VERSION                                        as                                        ICD_VERSION 
				FROM     LOGIC_MD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_MD                             as ( SELECT * from    RENAME_MD   ),

---- JOIN LAYER ----

 JOIN_MD  as  ( SELECT * 
				FROM  FILTER_MD )
 SELECT * FROM  JOIN_MD
  );
