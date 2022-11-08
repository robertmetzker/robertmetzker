---- SRC LAYER ----
WITH
SRC_HDR as ( SELECT *     from     STAGING.STG_INVOICE_HEADER ),
SRC_DIA as ( SELECT *     from     STAGING.STG_INVOICE_DIAGNOSIS ),
//SRC_HDR as ( SELECT *     from     STG_INVOICE_HEADER) ,
//SRC_DIA as ( SELECT *     from     STG_INVOICE_DIAGNOSIS) ,

---- LOGIC LAYER ----

LOGIC_HDR as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID 
		, TRIM( INVOICE_NUMBER )                             as                                     INVOICE_NUMBER 
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE 
		, SERVICE_FROM                                       as                                       SERVICE_FROM 
		, SERVICE_TO                                         as                                         SERVICE_TO 
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER 
		from SRC_HDR
            ),
LOGIC_DIA as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID 
		, SEQUENCE_NUMBER                                    as                                    SEQUENCE_NUMBER 
		, TRIM( DIAGNOSIS_CODE )                             as                                     DIAGNOSIS_CODE 
		, TRIM( PRESENT_ON_ADMIT )                           as                                   PRESENT_ON_ADMIT 
		, TRIM( REF_DSC )                                    as                                            REF_DSC 
		from SRC_DIA
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
RENAME_DIA as ( SELECT 
		  INVOICE_HEADER_ID                                  as                              DIA_INVOICE_HEADER_ID
		, SEQUENCE_NUMBER                                    as                                    SEQUENCE_NUMBER
		, DIAGNOSIS_CODE                                     as                                     DIAGNOSIS_CODE
		, PRESENT_ON_ADMIT                                   as                                   PRESENT_ON_ADMIT
		, REF_DSC                                            as                                            REF_DSC 
				FROM     LOGIC_DIA   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_HDR                            as ( SELECT * from    RENAME_HDR   ),
FILTER_DIA                            as ( SELECT * from    RENAME_DIA   ),

---- JOIN LAYER ----

HDR as ( SELECT * 
				FROM  FILTER_HDR
				LEFT JOIN FILTER_DIA ON  FILTER_HDR.INVOICE_HEADER_ID =  FILTER_DIA.DIA_INVOICE_HEADER_ID  )
SELECT * 
from HDR