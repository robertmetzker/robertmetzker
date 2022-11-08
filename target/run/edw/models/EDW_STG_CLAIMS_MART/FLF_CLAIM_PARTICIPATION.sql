

      create or replace  table DEV_EDW.EDW_STG_CLAIMS_MART.FLF_CLAIM_PARTICIPATION  as
      (

---- SRC LAYER ----
WITH
SRC_PART as ( SELECT *     from     STAGING.DSV_CLAIM_PARTICIPATION ),
SRC_CUST as ( SELECT *     from     EDW_STAGING_DIM.DIM_CUSTOMER ),
//SRC_PART as ( SELECT *     from     DSV_CLAIM_PARTICIPATION) ,
//SRC_CUST as ( SELECT *     from     DIM_CUSTOMER) ,

---- LOGIC LAYER ----


LOGIC_PART as ( SELECT 
		  PARTICIPATION_ID                                   as                                   PARTICIPATION_ID 
		,  CLAIM_NUMBER										 as                                       CLAIM_NUMBER               
		, CASE WHEN nullif(array_to_string(array_construct_compact(PARTICIPATION_TYPE_CODE,PARTICIPATION_PRIMARY_IND),''), '') is NULL  
				THEN MD5( '99999' ) ELSE 
		 md5(cast(
    
    coalesce(cast(PARTICIPATION_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(PARTICIPATION_PRIMARY_IND as 
    varchar
), '')

 as 
    varchar
))                                     
                END                                          as                            PARTICIPATION_TYPE_HKEY                                                 
		, CLAIM_PARTICIPATION_EFFECTIVE_DATE                 as                   PARTICIPATION_EFFECTIVE_DATE_KEY
		, CLAIM_PARTICIPATION_END_DATE                       as                         PARTICIPATION_END_DATE_KEY
		, AUDIT_USER_CREATE_DATE                             as                                    CREATE_DATE_KEY
		, AUDIT_USER_UPDATE_DATE                             as                                    UPDATE_DATE_KEY 
		,  CASE WHEN CREATE_USER_LOGIN_NAME IS NULL THEN md5('99999')
		   ELSE md5(cast(
    
    coalesce(cast(CREATE_USER_LOGIN_NAME as 
    varchar
), '')

 as 
    varchar
))                                  
													END		 as                                   CREATE_USER_HKEY
		, CASE WHEN UPDATE_USER_LOGIN_NAME IS NULL THEN md5('99999')
		   ELSE md5(cast(
    
    coalesce(cast(UPDATE_USER_LOGIN_NAME as 
    varchar
), '')

 as 
    varchar
))                                  
													END		 as   								  UPDATE_USER_HKEY
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
		, PARTICIPATION_TYPE_CODE                            as                            PARTICIPATION_TYPE_CODE 
		, PARTICIPATION_PRIMARY_IND                          as                          PARTICIPATION_PRIMARY_IND 
		, CREATE_USER_LOGIN_NAME                             as                             CREATE_USER_LOGIN_NAME 
		, UPDATE_USER_LOGIN_NAME                             as                             UPDATE_USER_LOGIN_NAME 
		, CLAIM_PARTICIPATION_EFFECTIVE_DATE                 as                 CLAIM_PARTICIPATION_EFFECTIVE_DATE 
		, CLAIM_PARTICIPATION_END_DATE                       as                       CLAIM_PARTICIPATION_END_DATE 
		, AUDIT_USER_CREATE_DATE                             as                             AUDIT_USER_CREATE_DATE 
		, AUDIT_USER_UPDATE_DATE                             as                             AUDIT_USER_UPDATE_DATE 
		from SRC_PART
            ),

LOGIC_CUST as ( SELECT 
		  CUSTOMER_HKEY                                      as                                      CUSTOMER_HKEY 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		from SRC_CUST
            )

---- RENAME LAYER ----
,

RENAME_PART as ( SELECT 
		  PARTICIPATION_ID                                   as                                   PARTICIPATION_ID
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, PARTICIPATION_TYPE_HKEY                            as                            PARTICIPATION_TYPE_HKEY
		, PARTICIPATION_EFFECTIVE_DATE_KEY                   as                   PARTICIPATION_EFFECTIVE_DATE_KEY
		, PARTICIPATION_END_DATE_KEY                         as                         PARTICIPATION_END_DATE_KEY
		, CREATE_DATE_KEY                                    as                                    CREATE_DATE_KEY
		, UPDATE_DATE_KEY                                    as                                    UPDATE_DATE_KEY
		, CREATE_USER_HKEY                                   as                                   CREATE_USER_HKEY
		, UPDATE_USER_HKEY                                   as                                   UPDATE_USER_HKEY
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER
		, PARTICIPATION_TYPE_CODE                            as                            PARTICIPATION_TYPE_CODE
		, PARTICIPATION_PRIMARY_IND                          as                          PARTICIPATION_PRIMARY_IND
		, CREATE_USER_LOGIN_NAME                             as                             CREATE_USER_LOGIN_NAME
		, UPDATE_USER_LOGIN_NAME                             as                             UPDATE_USER_LOGIN_NAME
		, CLAIM_PARTICIPATION_EFFECTIVE_DATE                 as                 CLAIM_PARTICIPATION_EFFECTIVE_DATE
		, CLAIM_PARTICIPATION_END_DATE                       as                       CLAIM_PARTICIPATION_END_DATE
		, AUDIT_USER_CREATE_DATE                             as                             AUDIT_USER_CREATE_DATE
		, AUDIT_USER_UPDATE_DATE                             as                             AUDIT_USER_UPDATE_DATE 
				FROM     LOGIC_PART   ), 
RENAME_CUST as ( SELECT 
		  CUSTOMER_HKEY                                      as                                      CUSTOMER_HKEY
		, CUSTOMER_NUMBER                                    as                               CUST_CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
				FROM     LOGIC_CUST   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PART                           as ( SELECT * from    RENAME_PART   ),
FILTER_CUST                           as ( SELECT * from    RENAME_CUST   ),

---- JOIN LAYER ----

PART as ( SELECT * 
				FROM  FILTER_PART
				LEFT JOIN FILTER_CUST ON  coalesce( FILTER_PART.CUSTOMER_NUMBER, 'UNK') =  FILTER_CUST.CUST_CUSTOMER_NUMBER AND coalesce(CLAIM_PARTICIPATION_END_DATE,'2099-12-31') BETWEEN RECORD_EFFECTIVE_DATE AND coalesce(RECORD_END_DATE, '2099-12-31')  )
SELECT 
		  PARTICIPATION_ID
		, CLAIM_NUMBER
		, coalesce( PARTICIPATION_TYPE_HKEY, MD5( '-1111' )) AS PARTICIPATION_TYPE_HKEY
		, coalesce( CUSTOMER_HKEY, MD5( '-1111' )) AS CUSTOMER_HKEY
		, CASE WHEN PARTICIPATION_EFFECTIVE_DATE_KEY is null then '-1' 
			WHEN PARTICIPATION_EFFECTIVE_DATE_KEY < '1901-01-01' then '-2' 
			WHEN PARTICIPATION_EFFECTIVE_DATE_KEY > '2099-12-31' then '-3' 
			ELSE regexp_replace( PARTICIPATION_EFFECTIVE_DATE_KEY, '[^0-9]+', '') 
				END :: INTEGER AS PARTICIPATION_EFFECTIVE_DATE_KEY
		, CASE WHEN PARTICIPATION_END_DATE_KEY is null then '-1' 
			WHEN PARTICIPATION_END_DATE_KEY < '1901-01-01' then '-2' 
			WHEN PARTICIPATION_END_DATE_KEY > '2099-12-31' then '-3' 
			ELSE regexp_replace( PARTICIPATION_END_DATE_KEY, '[^0-9]+', '') 
				END :: INTEGER  as PARTICIPATION_END_DATE_KEY
		, CASE WHEN CREATE_DATE_KEY is null then '-1' 
			WHEN CREATE_DATE_KEY < '1901-01-01' then '-2' 
			WHEN CREATE_DATE_KEY > '2099-12-31' then '-3' 
			ELSE regexp_replace( CREATE_DATE_KEY, '[^0-9]+', '') 
				END :: INTEGER as CREATE_DATE_KEY
		, CASE WHEN UPDATE_DATE_KEY is null then '-1' 
			WHEN UPDATE_DATE_KEY < '1901-01-01' then '-2' 
			WHEN UPDATE_DATE_KEY > '2099-12-31' then '-3' 
			ELSE regexp_replace( UPDATE_DATE_KEY, '[^0-9]+', '') 
				END :: INTEGER  as UPDATE_DATE_KEY
		, coalesce( CREATE_USER_HKEY, MD5( '-1111' )) as CREATE_USER_HKEY
		, coalesce( UPDATE_USER_HKEY, MD5( '-1111' )) as UPDATE_USER_HKEY 
        , CURRENT_TIMESTAMP AS LOAD_DATETIME
        , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
        , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
from PART
      );
    