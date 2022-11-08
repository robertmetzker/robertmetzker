

---- SRC LAYER ----
WITH
SRC_CLM as ( SELECT *     from     STAGING.DSV_CLAIM_ACTIVITY ),
//SRC_CLM as ( SELECT *     from     DSV_CLAIM_ACTIVITY) ,

---- LOGIC LAYER ----

LOGIC_CLM as ( SELECT 
		  ACTIVITY_ID                                        as                                        ACTIVITY_ID 
		, ACTIVITY_DETAIL_ID                                 as                                 ACTIVITY_DETAIL_ID                                                   
        , AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM                                                 
		, USER_LGN_NM                                        as                                        USER_LGN_NM 
		, ACTION_TYPE                                        as                                        ACTION_TYPE 
		, ACTIVITY_NAME_TYPE                                 as                                 ACTIVITY_NAME_TYPE 
		, ACTIVITY_CONTEXT_TYPE_NAME                         as                         ACTIVITY_CONTEXT_TYPE_NAME 
		, ACTIVITY_SUBCONTEXT_TYPE_NAME                      as                      ACTIVITY_SUBCONTEXT_TYPE_NAME 
		, PROCESS_AREA                                       as                                       PROCESS_AREA 
		, ACTIVITY_DETAIL_DESC                               as                               ACTIVITY_DETAIL_DESC 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		from SRC_CLM
            )

---- RENAME LAYER ----
,

RENAME_CLM as ( SELECT 
		  ACTIVITY_ID                                        as                                        ACTIVITY_ID
		, ACTIVITY_DETAIL_ID                                 as                                 ACTIVITY_DETAIL_ID	
        , AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM    		
		, USER_LGN_NM                                        as                                        USER_LGN_NM
		, ACTION_TYPE                                        as                                        ACTION_TYPE
		, ACTIVITY_NAME_TYPE                                 as                                 ACTIVITY_NAME_TYPE
		, ACTIVITY_CONTEXT_TYPE_NAME                         as                         ACTIVITY_CONTEXT_TYPE_NAME
		, ACTIVITY_SUBCONTEXT_TYPE_NAME                      as                      ACTIVITY_SUBCONTEXT_TYPE_NAME
		, PROCESS_AREA                                       as                                       PROCESS_AREA
		, ACTIVITY_DETAIL_DESC                               as                               ACTIVITY_DETAIL_DESC
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
				FROM     LOGIC_CLM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),

---- JOIN LAYER ----

 JOIN_CLM  as  ( SELECT * 
				FROM  FILTER_CLM )
				
----- ETL LAYER ----				
				
 SELECT  
  ACTIVITY_ID
, ACTIVITY_DETAIL_ID
, CLAIM_NUMBER
, md5(cast(
    
    coalesce(cast(USER_LGN_NM as 
    varchar
), '')

 as 
    varchar
)) AS USER_HKEY
, CASE WHEN  nullif(array_to_string(array_construct_compact( ACTION_TYPE, ACTIVITY_NAME_TYPE, ACTIVITY_CONTEXT_TYPE_NAME, ACTIVITY_SUBCONTEXT_TYPE_NAME, PROCESS_AREA),''),'') is NULL 
    THEN md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
    ELSE md5(cast(
    
    coalesce(cast(ACTION_TYPE as 
    varchar
), '') || '-' || coalesce(cast(ACTIVITY_NAME_TYPE as 
    varchar
), '') || '-' || coalesce(cast(ACTIVITY_CONTEXT_TYPE_NAME as 
    varchar
), '') || '-' || coalesce(cast(ACTIVITY_SUBCONTEXT_TYPE_NAME as 
    varchar
), '') || '-' || coalesce(cast(PROCESS_AREA as 
    varchar
), '')

 as 
    varchar
)) 
    END AS   ACTIVITY_HKEY
, CASE WHEN ACTIVITY_DETAIL_DESC is null then md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
        else md5(cast(
    
    coalesce(cast(ACTIVITY_DETAIL_DESC as 
    varchar
), '')

 as 
    varchar
)) END AS ACTIVITY_DETAIL_HKEY
, CAST(REPLACE(SUBSTR(AUDIT_USER_CREA_DTM::TIMESTAMP_NTZ, 11,9),':','' )  AS INT) AS ACTIVITY_TIME_KEY
, CAST(REPLACE(SUBSTR(AUDIT_USER_CREA_DTM::TIMESTAMP_NTZ, 1,10),'-','' )  AS INT) AS COMPLETED_DATE_KEY
, CURRENT_TIMESTAMP() AS LOAD_DATETIME
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM
FROM  JOIN_CLM