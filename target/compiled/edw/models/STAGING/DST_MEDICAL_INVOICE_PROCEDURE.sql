---- SRC LAYER ----
WITH
SRC_HDR as ( SELECT *     from     STAGING.STG_INVOICE_HEADER ),
SRC_PRO as ( SELECT *     from     STAGING.STG_HOSPITAL_PROCEDURE ),
//SRC_HDR as ( SELECT *     from     STG_INVOICE_HEADER) ,
//SRC_PRO as ( SELECT *     from     STG_HOSPITAL_PROCEDURE) ,

---- LOGIC LAYER ----

LOGIC_HDR as ( SELECT 
		  INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, TRIM( INVOICE_NUMBER )                             AS                                     INVOICE_NUMBER 
		, RECEIPT_DATE                                       AS                                       RECEIPT_DATE 
		, SERVICE_FROM                                       AS                                       SERVICE_FROM 
		, SERVICE_TO                                         AS                                         SERVICE_TO 
		, BATCH_NUMBER                                       AS                                       BATCH_NUMBER 
		from SRC_HDR
            ),
LOGIC_PRO as ( SELECT 
		  INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, SEQUENCE_NUMBER                                    AS                                    SEQUENCE_NUMBER 
		, TRIM( PROCEDURE_CODE )                             AS                                     PROCEDURE_CODE 
		, PROCEDURE_DATE                                     AS                                     PROCEDURE_DATE 
		, ICD_VERSION                                        AS                                        ICD_VERSION 
		from SRC_PRO
            )

---- RENAME LAYER ----
,

RENAME_HDR as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID
		, INVOICE_NUMBER                                     as                                     INVOICE_NUMBER
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE
		, SERVICE_FROM                                       as                                       SERVICE_FROM
		, SERVICE_TO                                         as                                         SERVICE_TO
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER 
				FROM     LOGIC_HDR   ), 
RENAME_PRO as ( SELECT 
		  INVOICE_HEADER_ID                                  as                              PRO_INVOICE_HEADER_ID
		, SEQUENCE_NUMBER                                    as                                PRO_SEQUENCE_NUMBER
		, PROCEDURE_CODE                                     as                                     PROCEDURE_CODE
		, PROCEDURE_DATE                                     as                                     PROCEDURE_DATE
		, ICD_VERSION                                        as                                        ICD_VERSION 
				FROM     LOGIC_PRO   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_HDR                            as ( SELECT * from    RENAME_HDR   ),
FILTER_PRO                            as ( SELECT * from    RENAME_PRO   ),

---- JOIN LAYER ----

HDR as ( SELECT * 
				FROM  FILTER_HDR
				LEFT JOIN FILTER_PRO ON  FILTER_HDR.INVOICE_HEADER_ID =  FILTER_PRO.PRO_INVOICE_HEADER_ID  )
SELECT * 
from HDR