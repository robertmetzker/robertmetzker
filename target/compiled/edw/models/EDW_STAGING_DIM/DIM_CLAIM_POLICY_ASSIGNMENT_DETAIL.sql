
WITH 
SRC_RC as ( SELECT *      from      STAGING.DSV_CLAIM_POLICY_HISTORY )
//SRC_RC as ( SELECT *     from      DSV_CLAIM_POLICY_HISTORY)

---- LOGIC LAYER ----
,
LOGIC_RC as ( SELECT 
       md5(cast(
    
    coalesce(cast(CRNT_PLCY_IND as 
    varchar
), '') || '-' || coalesce(cast(CTL_ELEM_SUB_TYP_CD as 
    varchar
), '')

 as 
    varchar
))  AS                        UNIQUE_ID_KEY
		, TRIM( CRNT_PLCY_IND )                              AS                                      CRNT_PLCY_IND 
		, TRIM( CTL_ELEM_SUB_TYP_CD )                        AS                                CTL_ELEM_SUB_TYP_CD 
		, TRIM( CTL_ELEM_SUB_TYP_NM )                        AS                                CTL_ELEM_SUB_TYP_NM 
		from SRC_RC
            )

---- RENAME LAYER ----
,
RENAME_RC as ( SELECT 
              UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CRNT_PLCY_IND                                      as                           CURRENT_CLAIM_POLICY_IND
		, CTL_ELEM_SUB_TYP_CD                                as                                   POLICY_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                                   POLICY_TYPE_NAME
				FROM     LOGIC_RC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_RC                             as ( SELECT * from    RENAME_RC  )

--- ETL Layer --------
,
 ETL AS (select DISTINCT
         md5(cast(
    
    coalesce(cast(CURRENT_CLAIM_POLICY_IND as 
    varchar
), '') || '-' || coalesce(cast(POLICY_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) as CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY
       , UNIQUE_ID_KEY
       , CURRENT_CLAIM_POLICY_IND
       , POLICY_TYPE_CODE 
       , POLICY_TYPE_NAME
       , CURRENT_TIMESTAMP as LOAD_DATETIME
       , TRY_TO_TIMESTAMP('Invalid') as UPDATE_DATETIME
       , 'CORESUITE' as PRIMARY_SOURCE_SYSTEM
       from FILTER_RC
       )
 SELECT * FROM ETL