

      create or replace  table DEV_EDW.STAGING.STG_ACTIVITY_DETAIL  as
      (---- SRC LAYER ----
WITH
SRC_AD as ( SELECT *     from     DEV_VIEWS.PCMP.ACTIVITY_DETAIL ),
SRC_ANT as ( SELECT *     from     DEV_VIEWS.PCMP.ACTIVITY_NAME_TYPE ),
SRC_AAT as ( SELECT *     from     DEV_VIEWS.PCMP.ACTIVITY_ACTION_TYPE ),
//SRC_AD as ( SELECT *     from     ACTIVITY_DETAIL) ,
//SRC_ANT as ( SELECT *     from     ACTIVITY_NAME_TYPE) ,
//SRC_AAT as ( SELECT *     from     ACTIVITY_ACTION_TYPE) ,

---- LOGIC LAYER ----

LOGIC_AD as ( SELECT 
		  ACTV_DTL_ID                                        AS                                        ACTV_DTL_ID 
		, ACTV_ID                                            AS                                            ACTV_ID 
		, upper( TRIM( ACTV_NM_TYP_CD ) )                    AS                                     ACTV_NM_TYP_CD 
		, upper( TRIM( ACTV_ACTN_TYP_CD ) )                  AS                                   ACTV_ACTN_TYP_CD 
		, upper( TRIM( ACTV_DTL_DESC ) )                     AS                                      ACTV_DTL_DESC 
		, ACTV_DTL_SUB_CNTX_ID                               AS                               ACTV_DTL_SUB_CNTX_ID 
		, upper( TRIM( ACTV_DTL_COL_NM ) )                   AS                                    ACTV_DTL_COL_NM 
		from SRC_AD
            ),
LOGIC_ANT as ( SELECT 
		  upper( TRIM( ACTV_NM_TYP_NM ) )                    AS                                     ACTV_NM_TYP_NM 
		, upper( ACTV_NM_TYP_VSBL_IND )                      AS                               ACTV_NM_TYP_VSBL_IND 
		, upper( TRIM( ACTV_NM_TYP_CD ) )                    AS                                     ACTV_NM_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_ANT
            ),
LOGIC_AAT as ( SELECT 
		  upper( TRIM( ACTV_ACTN_TYP_NM ) )                  AS                                   ACTV_ACTN_TYP_NM 
		, upper( TRIM( ACTV_ACTN_TYP_CD ) )                  AS                                   ACTV_ACTN_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_AAT
            )

---- RENAME LAYER ----
,

RENAME_AD as ( SELECT 
		  ACTV_DTL_ID                                        as                                        ACTV_DTL_ID
		, ACTV_ID                                            as                                            ACTV_ID
		, ACTV_NM_TYP_CD                                     as                                     ACTV_NM_TYP_CD
		, ACTV_ACTN_TYP_CD                                   as                                   ACTV_ACTN_TYP_CD
		, ACTV_DTL_DESC                                      as                                      ACTV_DTL_DESC
		, ACTV_DTL_SUB_CNTX_ID                               as                               ACTV_DTL_SUB_CNTX_ID
		, ACTV_DTL_COL_NM                                    as                                    ACTV_DTL_COL_NM 
				FROM     LOGIC_AD   ), 
RENAME_ANT as ( SELECT 
		  ACTV_NM_TYP_NM                                     as                                     ACTV_NM_TYP_NM
		, ACTV_NM_TYP_VSBL_IND                               as                               ACTV_NM_TYP_VSBL_IND
		, ACTV_NM_TYP_CD                                     as                                 ANT_ACTV_NM_TYP_CD
		, VOID_IND                                           as                                       ANT_VOID_IND 
				FROM     LOGIC_ANT   ), 
RENAME_AAT as ( SELECT 
		  ACTV_ACTN_TYP_NM                                   as                                   ACTV_ACTN_TYP_NM
		, ACTV_ACTN_TYP_CD                                   as                               AAT_ACTV_ACTN_TYP_CD
		, VOID_IND                                           as                                       AAT_VOID_IND 
				FROM     LOGIC_AAT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_AD                             as ( SELECT * from    RENAME_AD   ),
FILTER_AAT                            as ( SELECT * from    RENAME_AAT 
				WHERE AAT_VOID_IND = 'N'  ),
FILTER_ANT                            as ( SELECT * from    RENAME_ANT 
				WHERE ANT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

AD as ( SELECT * 
				FROM  FILTER_AD
				LEFT JOIN FILTER_AAT ON  FILTER_AD.ACTV_ACTN_TYP_CD =  FILTER_AAT.AAT_ACTV_ACTN_TYP_CD 
								LEFT JOIN FILTER_ANT ON  FILTER_AD.ACTV_NM_TYP_CD =  FILTER_ANT.ANT_ACTV_NM_TYP_CD  )
SELECT * 
from AD
      );
    