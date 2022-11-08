

      create or replace  table DEV_EDW.EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_DIAGNOSIS  as
      (

---- SRC LAYER ----
WITH
SRC_MD as ( SELECT *     from     STAGING.DSV_MEDICAL_INVOICE_DIAGNOSIS ),
SRC_ICD as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_ICD_ADM_PRE as ( SELECT *     from   EDW_STAGING_DIM.DIM_ICD_ADMISSION_PRESENCE ),
//SRC_MD as ( SELECT *     from     DSV_MEDICAL_INVOICE_DIAGNOSIS) ,
//SRC_ICD as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD) ,
//SRC_ICD_ADM_PRE as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD_ADMISSION_PRESENCE) ,

---- LOGIC LAYER ----

LOGIC_MD as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER 
		, CASE WHEN PRESENT_ON_ADMIT IS NULL THEN md5('99999')
		   ELSE md5(PRESENT_ON_ADMIT)                                  
													END		 as                          PRESENT_ON_ADMISSION_HKEY
        , PRESENT_ON_ADMIT                      	         as                          PRESENT_ON_ADMISSION_CODE
		, BWC_BILL_RECEIPT_DATE                              as                          BWC_BILL_RECEIPT_DATE_KEY 
		, INVOICE_SERVICE_FROM_DATE                          as                      INVOICE_SERVICE_FROM_DATE_KEY
        , INVOICE_SERVICE_FROM_DATE                          as                                  SERVICE_FROM_DATE
		, INVOICE_SERVICE_TO_DATE                            as                        INVOICE_SERVICE_TO_DATE_KEY 
		, DIAGNOSIS_SEQUENCE_NUMBER                          as                          DIAGNOSIS_SEQUENCE_NUMBER 
		, BATCH_NUMBER                                       as                               INVOICE_BATCH_NUMBER 
		, DIAGNOSIS_CODE                                     as                                     DIAGNOSIS_CODE 
		from SRC_MD
            ),
LOGIC_ICD as ( SELECT 
		  ICD_HKEY                                           as                              HEADER_DIAGNOSIS_HKEY 
		, ICD_CODE                                           as                                           ICD_CODE 
		, ICD_CODE_VERSION_NUMBER                            as                            ICD_CODE_VERSION_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                                 ICD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                       ICD_END_DATE
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		from SRC_ICD
            )

---- RENAME LAYER ----
,

RENAME_MD as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, PRESENT_ON_ADMISSION_HKEY                          as                          PRESENT_ON_ADMISSION_HKEY
        , PRESENT_ON_ADMISSION_CODE                          as                          PRESENT_ON_ADMISSION_CODE
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY
		, INVOICE_SERVICE_FROM_DATE_KEY                      as                      INVOICE_SERVICE_FROM_DATE_KEY
        , SERVICE_FROM_DATE                                  as                                  SERVICE_FROM_DATE
		, INVOICE_SERVICE_TO_DATE_KEY                        as                        INVOICE_SERVICE_TO_DATE_KEY
		, DIAGNOSIS_SEQUENCE_NUMBER                          as                          DIAGNOSIS_SEQUENCE_NUMBER
		, INVOICE_BATCH_NUMBER                               as                               INVOICE_BATCH_NUMBER
		, DIAGNOSIS_CODE                                     as                                     DIAGNOSIS_CODE 
				FROM     LOGIC_MD   ), 
RENAME_ICD as ( SELECT 
		  HEADER_DIAGNOSIS_HKEY                              as                              HEADER_DIAGNOSIS_HKEY
		, ICD_CODE                                           as                                           ICD_CODE
		, ICD_CODE_VERSION_NUMBER                            as                            ICD_CODE_VERSION_NUMBER
		, ICD_EFFECTIVE_DATE                                 as                                 ICD_EFFECTIVE_DATE
		, ICD_END_DATE                                       as                                       ICD_END_DATE
        , CURRENT_RECORD_IND                                 as                             ICD_CURRENT_RECORD_IND
				FROM     LOGIC_ICD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_MD                             as ( SELECT * from    RENAME_MD
                                            WHERE DIAGNOSIS_CODE is not null  ),
FILTER_ICD                            as ( SELECT * from    RENAME_ICD   ),

---- JOIN LAYER ----

MED_DATERANGE as ( SELECT * 
				FROM  FILTER_MD
				LEFT JOIN FILTER_ICD ON  coalesce( FILTER_MD.DIAGNOSIS_CODE,  'UNK') =  FILTER_ICD.ICD_CODE AND FILTER_MD.SERVICE_FROM_DATE BETWEEN FILTER_ICD.ICD_EFFECTIVE_DATE AND coalesce( FILTER_ICD.ICD_END_DATE, '2099-12-31') ),
NOT_IN_DATERANGE as ( SELECT MED_DATERANGE.* 
                      , FILTER_ICD1.HEADER_DIAGNOSIS_HKEY AS FILTER_ICD1_HEADER_DIAGNOSIS_HKEY
                      FROM MED_DATERANGE
				      LEFT JOIN FILTER_ICD FILTER_ICD1 ON  coalesce( MED_DATERANGE.DIAGNOSIS_CODE,  'Z') =  FILTER_ICD1.ICD_CODE AND FILTER_ICD1.ICD_CURRENT_RECORD_IND = 'Y'
				  			LEFT JOIN SRC_ICD_ADM_PRE ON coalesce(MED_DATERANGE.PRESENT_ON_ADMISSION_CODE,'UNK') = SRC_ICD_ADM_PRE.PRESENT_ON_ADMISSION_CODE
				  ) ,

ETL AS (
SELECT 
  MEDICAL_INVOICE_NUMBER
, CASE WHEN HEADER_DIAGNOSIS_HKEY IS NOT NULL THEN HEADER_DIAGNOSIS_HKEY
                WHEN FILTER_ICD1_HEADER_DIAGNOSIS_HKEY IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS HEADER_DIAGNOSIS_HKEY 
, COALESCE(NOT_IN_DATERANGE.PRESENT_ON_ADMISSION_HKEY, md5('-1111')) AS PRESENT_ON_ADMISSION_HKEY
, case when BWC_BILL_RECEIPT_DATE_KEY  is null then -1
    when replace(cast(BWC_BILL_RECEIPT_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(BWC_BILL_RECEIPT_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(BWC_BILL_RECEIPT_DATE_KEY ::DATE as varchar),'-','')::INTEGER 
        END AS BWC_BILL_RECEIPT_DATE_KEY
, case when INVOICE_SERVICE_FROM_DATE_KEY  is null then -1
    when replace(cast(INVOICE_SERVICE_FROM_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(INVOICE_SERVICE_FROM_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(INVOICE_SERVICE_FROM_DATE_KEY ::DATE as varchar),'-','')::INTEGER 
        END AS INVOICE_SERVICE_FROM_DATE_KEY
, case when INVOICE_SERVICE_TO_DATE_KEY  is null then -1
    when replace(cast(INVOICE_SERVICE_TO_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(INVOICE_SERVICE_TO_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(INVOICE_SERVICE_TO_DATE_KEY ::DATE as varchar),'-','')::INTEGER 
        END AS INVOICE_SERVICE_TO_DATE_KEY
, DIAGNOSIS_SEQUENCE_NUMBER
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
, 'CAM' AS PRIMARY_SOURCE_SYSTEM
, INVOICE_BATCH_NUMBER
from NOT_IN_DATERANGE
)
SELECT * 
from ETL
      );
    