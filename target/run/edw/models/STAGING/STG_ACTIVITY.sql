

      create or replace  table DEV_EDW.STAGING.STG_ACTIVITY  as
      (---- SRC LAYER ----
WITH
SRC_ACT as ( SELECT *     from     DEV_VIEWS.PCMP.ACTIVITY ),
SRC_SLT as ( SELECT *     from     DEV_VIEWS.PCMP.SUBLOCATION_TYPE ),
SRC_CT as ( SELECT *     from     DEV_VIEWS.PCMP.CONTEXT_TYPE ),
//SRC_ACT as ( SELECT *     from     ACTIVITY) ,
//SRC_SLT as ( SELECT *     from     SUBLOCATION_TYPE) ,
//SRC_CT as ( SELECT *     from     CONTEXT_TYPE) ,

---- LOGIC LAYER ----

LOGIC_ACT as ( SELECT 
		  ACTV_ID                                            AS                                            ACTV_ID 
		, upper( TRIM( SUBLOC_TYP_CD ) )                     AS                                      SUBLOC_TYP_CD 
		, upper( TRIM( CNTX_TYP_CD ) )                       AS                                        CNTX_TYP_CD 
		, ACTV_CNTX_ID                                       AS                                       ACTV_CNTX_ID 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		from SRC_ACT
            ),
LOGIC_SLT as ( SELECT 
		  upper( TRIM( SUBLOC_TYP_NM ) )                     AS                                      SUBLOC_TYP_NM 
		, upper( TRIM( SUBLOC_TYP_CD ) )                     AS                                      SUBLOC_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_SLT
            ),
LOGIC_CT as ( SELECT 
		  upper( TRIM( CNTX_TYP_NM ) )                       AS                                        CNTX_TYP_NM 
		, upper( TRIM( CNTX_TYP_CD ) )                       AS                                        CNTX_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CT
            )

---- RENAME LAYER ----
,

RENAME_ACT as ( SELECT 
		  ACTV_ID                                            as                                            ACTV_ID
		, SUBLOC_TYP_CD                                      as                                      SUBLOC_TYP_CD
		, CNTX_TYP_CD                                        as                                        CNTX_TYP_CD
		, ACTV_CNTX_ID                                       as                                       ACTV_CNTX_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
				FROM     LOGIC_ACT   ), 
RENAME_SLT as ( SELECT 
		  SUBLOC_TYP_NM                                      as                                      SUBLOC_TYP_NM
		, SUBLOC_TYP_CD                                      as                                  SLT_SUBLOC_TYP_CD
		, VOID_IND                                           as                                       SLT_VOID_IND 
				FROM     LOGIC_SLT   ), 
RENAME_CT as ( SELECT 
		  CNTX_TYP_NM                                        as                                        CNTX_TYP_NM
		, CNTX_TYP_CD                                        as                                     CT_CNTX_TYP_CD
		, VOID_IND                                           as                                        CT_VOID_IND 
				FROM     LOGIC_CT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ACT                            as ( SELECT * from    RENAME_ACT   ),
FILTER_SLT                            as ( SELECT * from    RENAME_SLT 
				WHERE SLT_VOID_IND = 'N'  ),
FILTER_CT                             as ( SELECT * from    RENAME_CT 
				WHERE CT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

ACT as ( SELECT * 
				FROM  FILTER_ACT
				LEFT JOIN FILTER_SLT ON  FILTER_ACT.SUBLOC_TYP_CD =  FILTER_SLT.SLT_SUBLOC_TYP_CD 
								LEFT JOIN FILTER_CT ON  FILTER_ACT.CNTX_TYP_CD =  FILTER_CT.CT_CNTX_TYP_CD  )
SELECT * 
from ACT
      );
    