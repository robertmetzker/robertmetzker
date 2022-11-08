

---- SRC LAYER ----
WITH
SRC_PROV as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_PES as ( SELECT *      from     EDW_STAGING_DIM.DIM_PROVIDER_ENROLLMENT_STATUS ),
SRC_PESL as ( SELECT *     from     STAGING.DSV_PROVIDER_ENROLLMENT_STATUS_LOG ),
//SRC_PROV as ( SELECT *   from     DIM_PROVIDER) ,
//SRC_PES as ( SELECT *    from     DIM_PROVIDER_ENROLLMENT_STATUS) ,
//SRC_PESL as ( SELECT *   from     DSV_PROVIDER_ENROLLMENT_STATUS_LOG) ,

---- LOGIC LAYER ----

LOGIC_PROV as ( SELECT 
		  PROVIDER_HKEY                                      AS                                      PROVIDER_HKEY 
		 , RECORD_EFFECTIVE_DATE                             AS                                      RECORD_EFFECTIVE_DATE 	
         , RECORD_END_DATE                                   AS                                      RECORD_END_DATE 
        , PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER		 
		from SRC_PROV
            ),
LOGIC_PES as ( SELECT 
		  ENROLLMENT_STATUS_HKEY                             AS                             ENROLLMENT_STATUS_HKEY 
         , ENROLLMENT_STATUS_TYPE_CODE                       AS                             ENROLLMENT_STATUS_TYPE_CODE
         , ENROLLMENT_STATUS_REASON_CODE                     AS                             ENROLLMENT_STATUS_REASON_CODE            
		from SRC_PES
            ),
LOGIC_PESL as ( SELECT 
		  DERIVED_EFFECTIVE_DATE                             AS                             DERIVED_EFFECTIVE_DATE 
		, DERIVED_ENDING_DATE                                AS                                DERIVED_ENDING_DATE 
		, ENROLLMENT_EFFECTIVE_DATE                          AS                          ENROLLMENT_EFFECTIVE_DATE 
		, ENROLLMENT_END_DATE                                AS                                ENROLLMENT_END_DATE 
		, PROVIDER_PEACH_NUMBER                              AS                              PROVIDER_PEACH_NUMBER 
		, ENROLLMENT_STATUS_TYPE_CODE                        AS                        ENROLLMENT_STATUS_TYPE_CODE 
		, ENROLLMENT_STATUS_REASON_CODE                      AS                      ENROLLMENT_STATUS_REASON_CODE 
		, ENROLLMENT_STATUS_TYPE_DESC                        AS                        ENROLLMENT_STATUS_TYPE_DESC 
		, ENROLLMENT_STATUS_REASON_DESC                      AS                      ENROLLMENT_STATUS_REASON_DESC 
		from SRC_PESL
            )

---- RENAME LAYER ----
,

RENAME_PROV as ( SELECT 
		  PROVIDER_HKEY                                      AS                             PROVIDER_HKEY 
		 , RECORD_EFFECTIVE_DATE                             AS                             PROV_RECORD_EFFECTIVE_DATE 
         , RECORD_END_DATE                                   AS                             PROV_RECORD_END_DATE 
        , PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER		 
		 FROM     LOGIC_PROV   ), 
RENAME_PES as ( SELECT 
		  ENROLLMENT_STATUS_HKEY                             AS                             ENROLLMENT_STATUS_HKEY 
         , ENROLLMENT_STATUS_TYPE_CODE                       AS                             ENROLLMENT_STATUS_TYPE_CODE
         , ENROLLMENT_STATUS_REASON_CODE                     AS                             ENROLLMENT_STATUS_REASON_CODE  
               
				FROM     LOGIC_PES   ), 
RENAME_PESL as ( SELECT 
		  DERIVED_EFFECTIVE_DATE                             AS                         DERIVED_EFFECTIVE_DATE_KEY
		, DERIVED_ENDING_DATE                                AS                            DERIVED_ENDING_DATE_KEY
		, ENROLLMENT_EFFECTIVE_DATE                          AS                      ENROLLMENT_EFFECTIVE_DATE_KEY
		, ENROLLMENT_END_DATE                                AS                            ENROLLMENT_END_DATE_KEY
		, PROVIDER_PEACH_NUMBER                              AS                              PROVIDER_PEACH_NUMBER
		, ENROLLMENT_STATUS_TYPE_CODE                        AS                        ENROLLMENT_STATUS_TYPE_CODE
		, ENROLLMENT_STATUS_REASON_CODE                      AS                      ENROLLMENT_STATUS_REASON_CODE
		, ENROLLMENT_STATUS_TYPE_DESC                        AS                        ENROLLMENT_STATUS_TYPE_DESC
		, ENROLLMENT_STATUS_REASON_DESC                      AS                      ENROLLMENT_STATUS_REASON_DESC 
				FROM     LOGIC_PESL   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PESL                           as ( SELECT * from    RENAME_PESL   ),
FILTER_PROV                           as ( SELECT * from    RENAME_PROV   ),
FILTER_PES                            as ( SELECT * from    RENAME_PES   ),

---- JOIN LAYER ----

PESL as ( SELECT * 
				FROM  FILTER_PESL
				LEFT JOIN FILTER_PROV ON  FILTER_PESL.PROVIDER_PEACH_NUMBER =  FILTER_PROV.PROVIDER_PEACH_NUMBER 
				          AND FILTER_PESL.DERIVED_EFFECTIVE_DATE_KEY BETWEEN FILTER_PROV.PROV_RECORD_EFFECTIVE_DATE AND COALESCE(FILTER_PROV.PROV_RECORD_END_DATE, '2999-12-31')
				LEFT JOIN FILTER_PES ON  FILTER_PESL.ENROLLMENT_STATUS_TYPE_CODE =  FILTER_PES.ENROLLMENT_STATUS_TYPE_CODE 
                          and FILTER_PESL.ENROLLMENT_STATUS_REASON_CODE =  FILTER_PES.ENROLLMENT_STATUS_REASON_CODE  ),
-- ETL layer---

ETL AS (

SELECT  distinct
 coalesce( PROVIDER_HKEY, md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
)) ) AS PROVIDER_HKEY
,coalesce( ENROLLMENT_STATUS_HKEY, md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
)) ) AS ENROLLMENT_STATUS_HKEY
,case when DERIVED_EFFECTIVE_DATE_KEY  is null then -1
    when replace(cast(DERIVED_EFFECTIVE_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(DERIVED_EFFECTIVE_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(DERIVED_EFFECTIVE_DATE_KEY 	 ::DATE as varchar),'-','')::INTEGER 
    END AS DERIVED_EFFECTIVE_DATE_KEY 
,case when DERIVED_ENDING_DATE_KEY  is null then -1
    when replace(cast(DERIVED_ENDING_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(DERIVED_ENDING_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(DERIVED_ENDING_DATE_KEY 	 ::DATE as varchar),'-','')::INTEGER 
    END AS DERIVED_ENDING_DATE_KEY 
,case when ENROLLMENT_EFFECTIVE_DATE_KEY  is null then -1
    when replace(cast(ENROLLMENT_EFFECTIVE_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(ENROLLMENT_EFFECTIVE_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(ENROLLMENT_EFFECTIVE_DATE_KEY 	 ::DATE as varchar),'-','')::INTEGER 
    END AS ENROLLMENT_EFFECTIVE_DATE_KEY 
,case when ENROLLMENT_END_DATE_KEY  is null then -1
    when replace(cast(ENROLLMENT_END_DATE_KEY ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(ENROLLMENT_END_DATE_KEY ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(ENROLLMENT_END_DATE_KEY 	 ::DATE as varchar),'-','')::INTEGER 
    END AS ENROLLMENT_END_DATE_KEY 
,CURRENT_TIMESTAMP AS LOAD_DATETIME
,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
,'PEACH' AS PRIMARY_SOURCE_SYSTEM 
from PESL
)

SELECT * 
from ETL