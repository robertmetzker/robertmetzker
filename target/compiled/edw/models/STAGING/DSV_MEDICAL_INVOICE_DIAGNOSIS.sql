

---- SRC LAYER ----
WITH
SRC_MD as ( SELECT *     from     STAGING.DST_MEDICAL_INVOICE_DIAGNOSIS ),
//SRC_MD as ( SELECT *     from     DST_MEDICAL_INVOICE_DIAGNOSIS) ,

---- LOGIC LAYER ----

LOGIC_MD as ( SELECT 
		  INVOICE_NUMBER                                     as                                     INVOICE_NUMBER 
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE 
		, SERVICE_FROM                                       as                                       SERVICE_FROM 
		, SERVICE_TO                                         as                                         SERVICE_TO 
		, SEQUENCE_NUMBER                                    as                                    SEQUENCE_NUMBER 
		, DIAGNOSIS_CODE                                     as                                     DIAGNOSIS_CODE 
		, PRESENT_ON_ADMIT                                   as                                   PRESENT_ON_ADMIT 
		, REF_DSC                                            as                                            REF_DSC 
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER 
		from SRC_MD
            )

---- RENAME LAYER ----
,

RENAME_MD as ( SELECT 
		  INVOICE_NUMBER                                     as                             MEDICAL_INVOICE_NUMBER
		, RECEIPT_DATE                                       as                              BWC_BILL_RECEIPT_DATE
		, SERVICE_FROM                                       as                          INVOICE_SERVICE_FROM_DATE
		, SERVICE_TO                                         as                            INVOICE_SERVICE_TO_DATE
		, SEQUENCE_NUMBER                                    as                          DIAGNOSIS_SEQUENCE_NUMBER
		, DIAGNOSIS_CODE                                     as                                     DIAGNOSIS_CODE
		, PRESENT_ON_ADMIT                                   as                                   PRESENT_ON_ADMIT
		, REF_DSC                                            as                          PRESENT_ON_ADMISSION_DESC
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER 
				FROM     LOGIC_MD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_MD                             as ( SELECT * from    RENAME_MD   ),

---- JOIN LAYER ----

 JOIN_MD  as  ( SELECT * 
				FROM  FILTER_MD )
 SELECT * FROM  JOIN_MD