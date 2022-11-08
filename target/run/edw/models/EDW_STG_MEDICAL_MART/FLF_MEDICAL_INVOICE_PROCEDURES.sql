

      create or replace  table DEV_EDW.EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_PROCEDURES  as
      (

---- SRC LAYER ----
WITH
SRC_MD as ( SELECT *     from     STAGING.DSV_MEDICAL_INVOICE_PROCEDURE ),
SRC_PRO as ( SELECT *     from     EDW_STAGING_DIM.DIM_HOSPITAL_ICD_PROCEDURE ),
//SRC_MD as ( SELECT *     from     DSV_MEDICAL_INVOICE_PROCEDURE) ,
//SRC_PRO as ( SELECT *     from    DIM_HOSPITAL_ICD_PROCEDURE) ,

---- LOGIC LAYER ----

LOGIC_MD as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             AS                             MEDICAL_INVOICE_NUMBER 
		, PROCEDURE_SEQUENCE_NUMBER                          AS                          PROCEDURE_SEQUENCE_NUMBER 
		, ICD_PROCEDURE_DATE                                 AS                                 ICD_PROCEDURE_DATE 
		, BWC_BILL_RECEIPT_DATE                              AS                              BWC_BILL_RECEIPT_DATE 
		, INVOICE_SERVICE_FROM_DATE                          AS                          INVOICE_SERVICE_FROM_DATE 
		, INVOICE_SERVICE_TO_DATE                            AS                            INVOICE_SERVICE_TO_DATE 
		, BATCH_NUMBER                                       AS                                       BATCH_NUMBER 
		, PROCEDURE_CODE                                     AS                                     PROCEDURE_CODE 
		, ICD_VERSION                                        AS                                        ICD_VERSION 
		from SRC_MD
            ),
LOGIC_PRO as ( SELECT 
		  ICD_PROCEDURE_HKEY                                           AS                        ICD_HKEY 
		, ICD_PROCEDURE_CODE                                 AS                                 ICD_PROCEDURE_CODE 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
         , CURRENT_RECORD_IND AS CURRENT_RECORD_IND     
		from SRC_PRO
            )

---- RENAME LAYER ----
,

RENAME_MD as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, PROCEDURE_SEQUENCE_NUMBER                          as                          PROCEDURE_SEQUENCE_NUMBER
		, ICD_PROCEDURE_DATE                                 as                             ICD_PROCEDURE_DATE
		, BWC_BILL_RECEIPT_DATE                              as                          BWC_BILL_RECEIPT_DATE
		, INVOICE_SERVICE_FROM_DATE                          as                      INVOICE_SERVICE_FROM_DATE
		, INVOICE_SERVICE_TO_DATE                            as                        INVOICE_SERVICE_TO_DATE
		, BATCH_NUMBER                                       as                               INVOICE_BATCH_NUMBER
		//, PROCEDURE_SEQUENCE_NUMBER                          as                          PROCEDURE_SEQUENCE_NUMBER
		, PROCEDURE_CODE                                     as                                     PROCEDURE_CODE
//		, ICD_PROCEDURE_DATE                                 as                                 ICD_PROCEDURE_DATE
		, ICD_VERSION                                        as                                        ICD_VERSION 
				FROM     LOGIC_MD   ), 
RENAME_PRO as ( SELECT 
		  ICD_HKEY                                           as                              HEADER_PROCEDURE_HKEY
		, ICD_PROCEDURE_CODE                                 as                                 ICD_PROCEDURE_CODE
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
         , CURRENT_RECORD_IND AS PRO_CURRENT_RECORD_IND     
				FROM     LOGIC_PRO   ),

---- FILTER LAYER (uses aliases) ----

FILTER_MD                             as ( SELECT * from    RENAME_MD 
				WHERE RENAME_MD.PROCEDURE_CODE IS NOT NULL  ),
FILTER_PRO                            as ( SELECT * from    RENAME_PRO   ),

---- JOIN LAYER ----

MD as ( SELECT * 
				FROM  FILTER_MD
				LEFT JOIN FILTER_PRO ON  coalesce(FILTER_MD.PROCEDURE_CODE,  'UNK') =  FILTER_PRO.ICD_PROCEDURE_CODE 
       AND INVOICE_SERVICE_TO_DATE BETWEEN RECORD_EFFECTIVE_DATE AND COALESCE(RECORD_END_DATE,'2099-12-31')  ),
       
      
       
-- ETL join layer to handle that are outside of date range MD5('-2222')                                                                
 ETL_FLF AS (SELECT MD.*, 
             FILTER_PRO.ICD_PROCEDURE_CODE  AS  FILTER_PRO_ICD_PROCEDURE_CODE, 
             FILTER_PRO.HEADER_PROCEDURE_HKEY AS FILTER_PRO_HEADER_PROCEDURE_HKEY
             
               
              FROM MD MD
             LEFT JOIN FILTER_PRO ON  coalesce(MD.PROCEDURE_CODE,  'UNK') =  FILTER_PRO.ICD_PROCEDURE_CODE 
               
				AND FILTER_PRO.PRO_CURRENT_RECORD_IND = 'Y'
              
                ),

                
-- ETL layer --


ETL AS (
SELECT  

MEDICAL_INVOICE_NUMBER,
  
CASE 
  when HEADER_PROCEDURE_HKEY is not null then HEADER_PROCEDURE_HKEY 
  when FILTER_PRO_HEADER_PROCEDURE_HKEY is null then md5(cast(
    
    coalesce(cast(-2222 as 
    varchar
), '')

 as 
    varchar
))
  else md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
)) end as  HEADER_PROCEDURE_HKEY,
  
PROCEDURE_SEQUENCE_NUMBER,
   case when ICD_PROCEDURE_DATE  is null then -1
    when replace(cast(ICD_PROCEDURE_DATE ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(ICD_PROCEDURE_DATE ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(ICD_PROCEDURE_DATE 	 ::DATE as varchar),'-','')::INTEGER 
        END AS ICD_PROCEDURE_DATE_KEY ,
  
   case when BWC_BILL_RECEIPT_DATE  is null then -1
    when replace(cast(BWC_BILL_RECEIPT_DATE ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(BWC_BILL_RECEIPT_DATE ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(BWC_BILL_RECEIPT_DATE 	 ::DATE as varchar),'-','')::INTEGER 
        END AS
BWC_BILL_RECEIPT_DATE_KEY,
 
     case when INVOICE_SERVICE_FROM_DATE  is null then -1
    when replace(cast(INVOICE_SERVICE_FROM_DATE ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(INVOICE_SERVICE_FROM_DATE ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(INVOICE_SERVICE_FROM_DATE 	 ::DATE as varchar),'-','')::INTEGER 
        END AS
INVOICE_SERVICE_FROM_DATE_KEY,
      case when INVOICE_SERVICE_TO_DATE  is null then -1
    when replace(cast(INVOICE_SERVICE_TO_DATE ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(INVOICE_SERVICE_TO_DATE ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(INVOICE_SERVICE_TO_DATE 	 ::DATE as varchar),'-','')::INTEGER 
        END AS 
INVOICE_SERVICE_TO_DATE_KEY,
  
  CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
, 'CAM' AS PRIMARY_SOURCE_SYSTEM,
  
INVOICE_BATCH_NUMBER

from ETL_FLF )

select * from ETL
      );
    