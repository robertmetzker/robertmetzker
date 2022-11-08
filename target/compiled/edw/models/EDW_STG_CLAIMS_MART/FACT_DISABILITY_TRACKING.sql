

---- SRC LAYER ----
WITH
SRC_DIS as ( SELECT *     from     STAGING.DSV_DISABILITY_MANAGEMENT ),
//SRC_DIS as ( SELECT *     from     DSV_DISABILITY_MANAGEMENT) ,

---- LOGIC LAYER ----

LOGIC_DIS as ( SELECT 
         DISABILITY_MANAGEMENT_ID                           as                           DISABILITY_MANAGEMENT_ID
		,   md5(cast(
    
    coalesce(cast(CLAIM_NUMBER as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                        CLAIM_HKEY                     
		   
		, CASE WHEN nullif(array_to_string(array_construct_compact( DISABILITY_TYPE_CODE,DISABILITY_REASON_TYPE_CODE,DISABILITY_MEDICAL_STATUS_TYPE_CODE,DISABILITY_WORK_STATUS_TYPE_CODE,CURRENT_DISABILITY_STATUS_IND),''), '') is NULL  
				THEN MD5( '99999' ) ELSE  
				md5(cast(
    
    coalesce(cast(DISABILITY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(DISABILITY_REASON_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(DISABILITY_MEDICAL_STATUS_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(DISABILITY_WORK_STATUS_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CURRENT_DISABILITY_STATUS_IND as 
    varchar
), '')

 as 
    varchar
)) 
				END				                             as                               DISABILITY_TYPE_HKEY                    


	, CASE WHEN DISABILITY_START_DATE is null then '-1' 
			WHEN DISABILITY_START_DATE < '1901-01-01' then '-2' 
			WHEN DISABILITY_START_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( DISABILITY_START_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                           DISABILITY_START_DATE_KEY 
		, CASE WHEN DISABILITY_END_DATE is null then '-1' 
			WHEN DISABILITY_END_DATE < '1901-01-01' then '-2' 
			WHEN DISABILITY_END_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( DISABILITY_END_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                             DISABILITY_END_DATE_KEY 
		, CASE WHEN AUDIT_USER_CREATE_DATE is null then '-1' 
			WHEN AUDIT_USER_CREATE_DATE < '1901-01-01' then '-2' 
			WHEN AUDIT_USER_CREATE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( AUDIT_USER_CREATE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                          DISABILITY_CREATE_DATE_KEY 
		, CASE WHEN AUDIT_USER_UPDATE_DATE is null then '-1' 
			WHEN AUDIT_USER_UPDATE_DATE < '1901-01-01' then '-2' 
			WHEN AUDIT_USER_UPDATE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( AUDIT_USER_UPDATE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                         DISABILITY_UPDATE_DATE_KEY 
		, md5(cast(
    
    coalesce(cast(CREATE_USER_LOGIN_NAME as 
    varchar
), '')

 as 
    varchar
))  
		                                                     as                                   CREATE_USER_HKEY 
		, md5(cast(
    
    coalesce(cast(UPDATE_USER_LOGIN_NAME as 
    varchar
), '')

 as 
    varchar
))                             
															 as                                   UPDATE_USER_HKEY 
		, DISABILITY_ACTUAL_ELAPSED_DAY_COUNT                as                DISABILITY_ACTUAL_ELAPSED_DAY_COUNT 
		, DISABILITY_CALENDAR_ELAPSED_DAY_COUNT              as              DISABILITY_CALENDAR_ELAPSED_DAY_COUNT 
        , DISABILITY_TYPE_CODE                               as                               DISABILITY_TYPE_CODE 
		, DISABILITY_REASON_TYPE_CODE                        as                        DISABILITY_REASON_TYPE_CODE 
		, DISABILITY_MEDICAL_STATUS_TYPE_CODE                as                DISABILITY_MEDICAL_STATUS_TYPE_CODE 
		, DISABILITY_WORK_STATUS_TYPE_CODE                   as                   DISABILITY_WORK_STATUS_TYPE_CODE 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, CURRENT_DISABILITY_STATUS_IND                      as                      CURRENT_DISABILITY_STATUS_IND
		from SRC_DIS

            )

---- RENAME LAYER ----
,

RENAME_DIS as ( SELECT 
          DISABILITY_MANAGEMENT_ID                           as                           DISABILITY_MANAGEMENT_ID
		, CLAIM_HKEY                                         as                                         CLAIM_HKEY
		, DISABILITY_TYPE_HKEY                               as                               DISABILITY_TYPE_HKEY
		, DISABILITY_START_DATE_KEY                          as                          DISABILITY_START_DATE_KEY
		, DISABILITY_END_DATE_KEY                            as                            DISABILITY_END_DATE_KEY
		, DISABILITY_CREATE_DATE_KEY                         as                         DISABILITY_CREATE_DATE_KEY
		, DISABILITY_UPDATE_DATE_KEY                         as                         DISABILITY_UPDATE_DATE_KEY
		, CREATE_USER_HKEY                                   as                                   CREATE_USER_HKEY
		, UPDATE_USER_HKEY                                   as                                   UPDATE_USER_HKEY
		, DISABILITY_ACTUAL_ELAPSED_DAY_COUNT                as               DISABILITY_ACTUAL_ELAPSED_DAYS_COUNT
		, DISABILITY_CALENDAR_ELAPSED_DAY_COUNT              as             DISABILITY_CALENDAR_ELAPSED_DAYS_COUNT
		, DISABILITY_TYPE_CODE                               as                               DISABILITY_TYPE_CODE
		, DISABILITY_REASON_TYPE_CODE                        as                        DISABILITY_REASON_TYPE_CODE
		, DISABILITY_MEDICAL_STATUS_TYPE_CODE                as                DISABILITY_MEDICAL_STATUS_TYPE_CODE
		, DISABILITY_WORK_STATUS_TYPE_CODE                   as                   DISABILITY_WORK_STATUS_TYPE_CODE
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, CURRENT_DISABILITY_STATUS_IND                      as                      CURRENT_DISABILITY_STATUS_IND
		FROM     LOGIC_DIS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DIS                            as ( SELECT * from    RENAME_DIS   ),

---- JOIN LAYER ----

 JOIN_DIS  as  ( SELECT *  FROM  FILTER_DIS )

---- ETL LAYER -----------

SELECT DISTINCT
  DISABILITY_MANAGEMENT_ID
, coalesce( DISABILITY_TYPE_HKEY, MD5( '-1111' )) as  DISABILITY_TYPE_HKEY
, DISABILITY_START_DATE_KEY
, DISABILITY_END_DATE_KEY
, DISABILITY_CREATE_DATE_KEY
, DISABILITY_UPDATE_DATE_KEY
, coalesce( CREATE_USER_HKEY, md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
)) ) AS CREATE_USER_HKEY
, coalesce( UPDATE_USER_HKEY, md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
)) ) AS UPDATE_USER_HKEY
, CLAIM_NUMBER
, DISABILITY_CALENDAR_ELAPSED_DAYS_COUNT
, DISABILITY_ACTUAL_ELAPSED_DAYS_COUNT
, SUM(DISABILITY_ACTUAL_ELAPSED_DAYS_COUNT) OVER (PARTITION BY CLAIM_NUMBER)  AS TOTAL_ELAPSED_DAY_COUNT
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
,'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
FROM  JOIN_DIS
/*group by 
  DISABILITY_MANAGEMENT_ID
, DISABILITY_TYPE_HKEY
, DISABILITY_START_DATE_KEY
, DISABILITY_END_DATE_KEY
, DISABILITY_CREATE_DATE_KEY
, DISABILITY_UPDATE_DATE_KEY
, CREATE_USER_HKEY
, UPDATE_USER_HKEY
, CLAIM_NUMBER
, DISABILITY_CALENDAR_ELAPSED_DAYS_COUNT
, DISABILITY_ACTUAL_ELAPSED_DAYS_COUNT
*/