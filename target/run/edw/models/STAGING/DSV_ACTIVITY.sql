
  create or replace  view DEV_EDW_32600145.STAGING.DSV_ACTIVITY  as (
    

---- SRC LAYER ----
WITH
SRC_AC as ( SELECT *     from     STAGING.DST_ACTIVITY ),
//SRC_AC as ( SELECT *     from     DST_ACTIVITY) ,

---- LOGIC LAYER ----

LOGIC_AC as ( SELECT 
		  md5(cast(
    
    coalesce(cast(ACTV_ACTN_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(ACTV_NM_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(CNTX_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(ACTV_DTL_COL_NM as 
    varchar
), '') || '-' || coalesce(cast(SUBLOC_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(FNCT_ROLE_NM as 
    varchar
), '')

 as 
    varchar
))                                      as                                  UNIQUE_ID_KEY                    
		, ACTV_ACTN_TYP_NM                                   as                                   ACTV_ACTN_TYP_NM 
		, ACTV_NM_TYP_NM                                     as                                     ACTV_NM_TYP_NM 
		, CNTX_TYP_NM                                        as                                        CNTX_TYP_NM 
		, ACTV_DTL_COL_NM                                    as                                    ACTV_DTL_COL_NM 
		, SUBLOC_TYP_NM                                      as                                      SUBLOC_TYP_NM 
		, FNCT_ROLE_NM                                       as                                       FNCT_ROLE_NM
		from SRC_AC
            )

---- RENAME LAYER ----
,

RENAME_AC as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, ACTV_ACTN_TYP_NM                                   as                                        ACTION_TYPE
		, ACTV_NM_TYP_NM                                     as                                 ACTIVITY_NAME_TYPE
		, CNTX_TYP_NM                                        as                         ACTIVITY_CONTEXT_TYPE_NAME
		, ACTV_DTL_COL_NM                                    as                      ACTIVITY_SUBCONTEXT_TYPE_NAME
		, SUBLOC_TYP_NM                                      as                                       PROCESS_AREA
		, FNCT_ROLE_NM                                       as                          USER_FUNCTIONAL_ROLE_DESC 
				FROM     LOGIC_AC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_AC                             as ( SELECT * from    RENAME_AC   ),

---- JOIN LAYER ----

 JOIN_AC  as  ( SELECT * 
				FROM  FILTER_AC )
 SELECT distinct
 UNIQUE_ID_KEY
,ACTION_TYPE
,ACTIVITY_NAME_TYPE
,ACTIVITY_CONTEXT_TYPE_NAME
,ACTIVITY_SUBCONTEXT_TYPE_NAME
,PROCESS_AREA 
,USER_FUNCTIONAL_ROLE_DESC
FROM  JOIN_AC
  );
