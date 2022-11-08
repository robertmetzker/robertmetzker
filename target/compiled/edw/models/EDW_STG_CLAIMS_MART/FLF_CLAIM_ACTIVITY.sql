

---- SRC LAYER ----
WITH
SRC_CLM            as ( SELECT *     FROM     STAGING.DSV_CLAIM_ACTIVITY ),
//SRC_CLM            as ( SELECT *     FROM     DSV_CLAIM_ACTIVITY) ,

---- LOGIC LAYER ----


LOGIC_CLM as ( SELECT 
		  ACTIVITY_ID                                        as                                        ACTIVITY_ID 
		, ACTIVITY_DETAIL_ID                                 as                                 ACTIVITY_DETAIL_ID 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		,  md5(cast(
    
    coalesce(cast(USER_LGN_NM as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                          USER_HKEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( ACTION_TYPE, ACTIVITY_NAME_TYPE, ACTIVITY_CONTEXT_TYPE_NAME, ACTIVITY_SUBCONTEXT_TYPE_NAME, PROCESS_AREA, USER_FUNCTIONAL_ROLE_DESC ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
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
), '') || '-' || coalesce(cast(USER_FUNCTIONAL_ROLE_DESC as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                                      ACTIVITY_HKEY 
		, CASE WHEN ACTIVITY_DETAIL_DESC is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(ACTIVITY_DETAIL_DESC as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                               ACTIVITY_DETAIL_HKEY 
        , AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, CASE WHEN CLAIM_INITIAL_FILE_DATE is null then '-1' 
			WHEN CLAIM_INITIAL_FILE_DATE < '1901-01-01' then '-2' 
			WHEN CLAIM_INITIAL_FILE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLAIM_INITIAL_FILE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                        CLAIM_INITIAL_FILE_DATE_KEY 
		, CASE WHEN CLM_OCCR_DATE is null then '-1' 
			WHEN CLM_OCCR_DATE < '1901-01-01' then '-2' 
			WHEN CLM_OCCR_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLM_OCCR_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                  CLM_OCCR_DATE_KEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( CURRENT_CORESUITE_CLAIM_TYPE_CODE, CLAIM_TYPE_CHNG_OVR_IND, CLAIM_STATE_CODE, CLAIM_STATUS_CODE, CLAIM_STATUS_REASON_CODE ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(CURRENT_CORESUITE_CLAIM_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_TYPE_CHNG_OVR_IND as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATUS_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATUS_REASON_CODE as 
    varchar
), '')

 as 
    varchar
)) 
				END                                          as                             CLAIM_TYPE_STATUS_HKEY 
		,  md5(cast(
    
    coalesce(cast(FILING_SOURCE_DESC as 
    varchar
), '') || '-' || coalesce(cast(FILING_MEDIA_DESC as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_CATEGORY as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_TYPE as 
    varchar
), '') || '-' || coalesce(cast(FIREFIGHTER_CANCER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EXPOSURE_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EMERGENCY_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_HEALTH_CARE_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COMBINED_IND as 
    varchar
), '') || '-' || coalesce(cast(SB223_IND as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYER_PREMISES_IND as 
    varchar
), '') || '-' || coalesce(cast(CATASTROPHIC_IND as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_ENROLLMENT_DESC as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_REASON_DESC as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                  CLAIM_DETAIL_HKEY 
		, CASE WHEN ORG_UNT_NM is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast( ORG_UNT_NM as 
    varchar
), '')

 as 
    varchar
))      
			 END                                             as                             ORGANIZATION_UNIT_HKEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( POLICY_TYPE_CODE, PLCY_STS_TYP_CD, PLCY_STS_RSN_TYP_CD, POLICY_ACTIVE_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(POLICY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(PLCY_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PLCY_STS_RSN_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(POLICY_ACTIVE_IND as 
    varchar
), '')

 as 
    varchar
)) 
				END                                          as                               POLICY_STANDING_HKEY 
		, POLICY_NUMBER                                      as                                      POLICY_NUMBER 
		, USER_LGN_NM                                        as                                        USER_LGN_NM 
		, ACTION_TYPE                                        as                                        ACTION_TYPE 
		, ACTIVITY_NAME_TYPE                                 as                                 ACTIVITY_NAME_TYPE 
		, ACTIVITY_CONTEXT_TYPE_NAME                         as                         ACTIVITY_CONTEXT_TYPE_NAME 
		, ACTIVITY_SUBCONTEXT_TYPE_NAME                      as                      ACTIVITY_SUBCONTEXT_TYPE_NAME 
		, PROCESS_AREA                                       as                                       PROCESS_AREA 
		, ACTIVITY_DETAIL_DESC                               as                               ACTIVITY_DETAIL_DESC 
		FROM SRC_CLM
            )

---- RENAME LAYER ----
,

RENAME_CLM        as ( SELECT 
		  ACTIVITY_ID                                        as                                        ACTIVITY_ID
		, ACTIVITY_DETAIL_ID                                 as                                 ACTIVITY_DETAIL_ID
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, USER_HKEY                                          as                                          USER_HKEY
		, ACTIVITY_HKEY                                      as                                      ACTIVITY_HKEY
		, ACTIVITY_DETAIL_HKEY                               as                               ACTIVITY_DETAIL_HKEY
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_CREA_DTM                                as                                 COMPLETED_DATE_KEY
		, CLAIM_INITIAL_FILE_DATE_KEY                        as                              INITIAL_FILE_DATE_KEY
		, CLM_OCCR_DATE_KEY                                  as                                OCCURRENCE_DATE_KEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, ORGANIZATION_UNIT_HKEY                             as                             ORGANIZATION_UNIT_HKEY
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY
		, POLICY_NUMBER                                      as                                      POLICY_NUMBER
		, USER_LGN_NM                                        as                                        USER_LGN_NM
		, ACTION_TYPE                                        as                                        ACTION_TYPE
		, ACTIVITY_NAME_TYPE                                 as                                 ACTIVITY_NAME_TYPE
		, ACTIVITY_CONTEXT_TYPE_NAME                         as                         ACTIVITY_CONTEXT_TYPE_NAME
		, ACTIVITY_SUBCONTEXT_TYPE_NAME                      as                      ACTIVITY_SUBCONTEXT_TYPE_NAME
		, PROCESS_AREA                                       as                                       PROCESS_AREA
		, ACTIVITY_DETAIL_DESC                               as                               ACTIVITY_DETAIL_DESC
				FROM     LOGIC_CLM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM                            as ( SELECT * FROM    RENAME_CLM   ),

---- JOIN LAYER ----

 JOIN_CLM         as  ( SELECT * 
				FROM  FILTER_CLM )
				
----- ETL LAYER ----				
				
 SELECT  
     ACTIVITY_ID
   , ACTIVITY_DETAIL_ID
   , CLAIM_NUMBER
   , USER_HKEY
   , ACTIVITY_HKEY
   , ACTIVITY_DETAIL_HKEY
   , CAST(REPLACE(SUBSTR(AUDIT_USER_CREA_DTM::TIMESTAMP_NTZ, 11,9),':','' )  AS INT) AS ACTIVITY_TIME_KEY
   , CAST(REPLACE(SUBSTR(AUDIT_USER_CREA_DTM::TIMESTAMP_NTZ, 1,10),'-','' )  AS INT) AS COMPLETED_DATE_KEY
   , INITIAL_FILE_DATE_KEY
   , OCCURRENCE_DATE_KEY
   , CLAIM_TYPE_STATUS_HKEY
   , CLAIM_DETAIL_HKEY
   , ORGANIZATION_UNIT_HKEY
   , POLICY_STANDING_HKEY
   , POLICY_NUMBER
   , CURRENT_TIMESTAMP() AS LOAD_DATETIME
   , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM
FROM  JOIN_CLM