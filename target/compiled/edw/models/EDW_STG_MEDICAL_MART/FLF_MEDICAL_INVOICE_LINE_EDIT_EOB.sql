

---- SRC LAYER ----
WITH
SRC_FLF as ( SELECT *     from     STAGING.DSV_MEDICAL_INVOICE_LINE_EDIT_EOB ),
SRC_EDIT as ( SELECT *     from     EDW_STAGING_DIM.DIM_EDIT ),
SRC_EOB as ( SELECT *     from     EDW_STAGING_DIM.DIM_EOB ),
//SRC_FLF as ( SELECT *     from     DSV_MEDICAL_INVOICE_LINE_EDIT_EOB) ,
//SRC_EDIT as ( SELECT *     from     DIMENSIONS.DIM_EDIT) ,
//SRC_EOB as ( SELECT *     from     DIMENSIONS.DIM_EOB) ,

---- LOGIC LAYER ----

LOGIC_FLF as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             AS                             MEDICAL_INVOICE_NUMBER 
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               AS               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER 
		, EDIT_ENTRY_DATE                                    AS                                    EDIT_ENTRY_DATE 
		, EOB_ENTRY_DATE                                     AS                                     EOB_ENTRY_DATE 
		, RECEIPT_DATE                                       AS                                       RECEIPT_DATE 
		, LINE_EDIT_SOURCE                                   AS                                   LINE_EDIT_SOURCE 
		, LINE_EDIT_PHASE_APPLIED                            AS                            LINE_EDIT_PHASE_APPLIED 
		, LINE_EDIT_DISPOSITION                              AS                              LINE_EDIT_DISPOSITION 
		, EDIT_CODE                                          AS                                          EDIT_CODE 
		, EOB_CODE                                           AS                                           EOB_CODE 
		, LINE_EOB_SOURCE                                    AS                                    LINE_EOB_SOURCE 
		, LINE_EOB_PHASE_APPLIED                             AS                             LINE_EOB_PHASE_APPLIED 
		, EOB_DISPOSITION                                    AS                                    EOB_DISPOSITION 

		from SRC_FLF
            ),
LOGIC_EDIT as ( SELECT 
		  EDIT_HKEY                                          AS                                          EDIT_HKEY 
		, EDIT_CODE                                          AS                                          EDIT_CODE 
		, EDIT_EFFECTIVE_DATE                                AS                                EDIT_EFFECTIVE_DATE 
		, EDIT_END_DATE                                      AS                                      EDIT_END_DATE 
         , CURRENT_RECORD_IND                                AS                                  CURRENT_RECORD_IND
          ,RECORD_EFFECTIVE_DATE                             AS                                  RECORD_EFFECTIVE_DATE
          ,RECORD_END_DATE                                   AS                                  RECORD_END_DATE
		from SRC_EDIT
            ),
LOGIC_EOB as ( SELECT 
		  EOB_HKEY                                           AS                                           EOB_HKEY 
		, EOB_CODE                                           AS                                           EOB_CODE 
		, EOB_EFFECTIVE_DATE                                 AS                                 EOB_EFFECTIVE_DATE 
		, EOB_END_DATE                                       AS                                       EOB_END_DATE 
        , CURRENT_RECORD_IND                                AS                                  CURRENT_RECORD_IND   
         ,RECORD_EFFECTIVE_DATE                             AS                                  RECORD_EFFECTIVE_DATE
          ,RECORD_END_DATE                                   AS                                  RECORD_END_DATE      
		from SRC_EOB
            )

---- RENAME LAYER ----
,

RENAME_FLF as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, EDIT_ENTRY_DATE                                    as                                EDIT_ENTRY_DATE
		, EOB_ENTRY_DATE                                     as                                 EOB_ENTRY_DATE		
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE
		, LINE_EDIT_SOURCE                                   as                                   LINE_EDIT_SOURCE
		, LINE_EDIT_PHASE_APPLIED                            as                            LINE_EDIT_PHASE_APPLIED
		, LINE_EDIT_DISPOSITION                              as                              LINE_EDIT_DISPOSITION
		, EDIT_CODE                                          as                                          EDIT_CODE
		, EOB_CODE                                           as                                           EOB_CODE
		, LINE_EOB_SOURCE                                    as                                    LINE_EOB_SOURCE
		, LINE_EOB_PHASE_APPLIED                             as                             LINE_EOB_PHASE_APPLIED
		, EOB_DISPOSITION                                    as                                    EOB_DISPOSITION

				FROM     LOGIC_FLF   ), 
RENAME_EDIT as ( SELECT 
		  EDIT_HKEY                                          as                                          EDIT_HKEY
		, EDIT_CODE                                          as                                      DIM_EDIT_CODE
		, EDIT_EFFECTIVE_DATE                                as                                EDIT_EFFECTIVE_DATE
		, EDIT_END_DATE                                      as                                      EDIT_END_DATE 
        , CURRENT_RECORD_IND                                AS                              EDIT_CURRENT_RECORD_IND  
         ,RECORD_EFFECTIVE_DATE                             AS                                  EDIT_RECORD_EFFECTIVE_DATE
          ,RECORD_END_DATE                                   AS                                  EDIT_RECORD_END_DATE       
				FROM     LOGIC_EDIT   ), 
RENAME_EOB as ( SELECT 
		  EOB_HKEY                                           as                                           EOB_HKEY
		, EOB_CODE                                           as                                       DIM_EOB_CODE
		, EOB_EFFECTIVE_DATE                                 as                                 EOB_EFFECTIVE_DATE
		, EOB_END_DATE                                       as                                       EOB_END_DATE 
        , CURRENT_RECORD_IND                                AS                                 EOB_CURRENT_RECORD_IND               
         ,RECORD_EFFECTIVE_DATE                             AS                                  EOB_RECORD_EFFECTIVE_DATE
          ,RECORD_END_DATE                                   AS                                  EOB_RECORD_END_DATE  
               FROM     LOGIC_EOB   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_FLF                            as ( SELECT * from    RENAME_FLF   ),
FILTER_EDIT                           as ( SELECT * from    RENAME_EDIT   ),
FILTER_EOB                            as ( SELECT * from    RENAME_EOB   ),

---- JOIN LAYER ----

FLF as ( SELECT * 
				FROM  FILTER_FLF
				LEFT JOIN FILTER_EDIT ON  FILTER_FLF.EDIT_CODE =  FILTER_EDIT.DIM_EDIT_CODE AND EDIT_ENTRY_DATE BETWEEN EDIT_RECORD_EFFECTIVE_DATE AND coalesce( EDIT_RECORD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_EOB ON  FILTER_FLF.EOB_CODE =  FILTER_EOB.DIM_EOB_CODE AND EOB_ENTRY_DATE BETWEEN EOB_RECORD_EFFECTIVE_DATE AND coalesce( EOB_RECORD_END_DATE, '2099-12-31')  ),



-- ETL join layer to handle that are outside of date range MD5('-2222')                                                                
 ETL_FLF AS (SELECT FILTER_FLF.*
                , FILTER_EDIT.DIM_EDIT_CODE AS FILTER_EDIT_EDIT_CODE
                , FILTER_EOB.DIM_EOB_CODE AS FILTER_EOB_EOB_CODE
              FROM FLF FILTER_FLF
                LEFT JOIN FILTER_EDIT ON   coalesce( FILTER_FLF.EDIT_CODE,  'Z') =  FILTER_EDIT.DIM_EDIT_CODE 
				AND FILTER_EDIT.EDIT_CURRENT_RECORD_IND = 'Y'
               LEFT JOIN FILTER_EOB ON   coalesce( FILTER_FLF.EOB_CODE,  'Z') =  FILTER_EOB.DIM_EOB_CODE 
				AND FILTER_EOB.EOB_CURRENT_RECORD_IND = 'Y'
                ),
								
--- ETL layer --

ETL AS (
select 
MEDICAL_INVOICE_NUMBER	,
MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER	,
CASE WHEN EDIT_HKEY IS NOT NULL THEN EDIT_HKEY
WHEN FILTER_EDIT_EDIT_CODE IS NOT NULL THEN md5(cast(
    
    coalesce(cast(-2222 as 
    varchar
), '')

 as 
    varchar
)) 
else md5(cast(
    
    coalesce(cast(88888 as 
    varchar
), '')

 as 
    varchar
))
END AS EDIT_HKEY,
CASE WHEN EOB_HKEY IS NOT NULL THEN EOB_HKEY	
WHEN FILTER_EOB_EOB_CODE IS NOT NULL THEN md5(cast(
    
    coalesce(cast(-2222 as 
    varchar
), '')

 as 
    varchar
)) 
else md5(cast(
    
    coalesce(cast(88888 as 
    varchar
), '')

 as 
    varchar
))
END AS EOB_HKEY	,
md5(cast(
    
    coalesce(cast(LINE_EDIT_SOURCE as 
    varchar
), '') || '-' || coalesce(cast(LINE_EDIT_PHASE_APPLIED as 
    varchar
), '') || '-' || coalesce(cast(LINE_EDIT_DISPOSITION as 
    varchar
), '')

 as 
    varchar
)) AS EDIT_ENTRY_HKEY, 
md5(cast(
    
    coalesce(cast(LINE_EOB_SOURCE as 
    varchar
), '') || '-' || coalesce(cast(LINE_EOB_PHASE_APPLIED as 
    varchar
), '') || '-' || coalesce(cast(EOB_DISPOSITION as 
    varchar
), '')

 as 
    varchar
)) AS EOB_ENTRY_HKEY,
 case when EDIT_ENTRY_DATE  is null then -1
    when replace(cast(EDIT_ENTRY_DATE ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(EDIT_ENTRY_DATE ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(EDIT_ENTRY_DATE 	 ::DATE as varchar),'-','')::INTEGER 
        END AS EDIT_ENTRY_DATE_KEY ,
case when EOB_ENTRY_DATE  is null then -1
    when replace(cast(EOB_ENTRY_DATE ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(EOB_ENTRY_DATE ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(EOB_ENTRY_DATE 	 ::DATE as varchar),'-','')::INTEGER 
        END AS EOB_ENTRY_DATE_KEY ,
CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
, 'CAM' AS PRIMARY_SOURCE_SYSTEM
 from ETL_FLF



)

SELECT *
from ETL