

      create or replace  table DEV_EDW.STAGING.STG_CASE_DETAIL_LEGAL  as
      (---- SRC LAYER ----
WITH
SRC_CDL as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_DETAIL_LEGAL ),
SRC_CJT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_JURISDICTION_TYPE ),
SRC_CVT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_VENUE_TYPE ),
SRC_CVLT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_VENUE_LOCATION_TYPE ),
//SRC_CDL as ( SELECT *     from     CASE_DETAIL_LEGAL) ,
//SRC_CJT as ( SELECT *     from     CASE_JURISDICTION_TYPE) ,
//SRC_CVT as ( SELECT *     from     CASE_VENUE_TYPE) ,
//SRC_CVLT as ( SELECT *     from     CASE_VENUE_LOCATION_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CDL as ( SELECT 
		  CDL_ID                                             AS                                             CDL_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, upper( TRIM( CASE_JUR_TYP_CD ) )                   AS                                    CASE_JUR_TYP_CD 
		, upper( TRIM( CASE_VENU_TYP_CD ) )                  AS                                   CASE_VENU_TYP_CD 
		, upper( TRIM( CASE_VENU_LOC_TYP_CD ) )              AS                               CASE_VENU_LOC_TYP_CD 
		, cast( CDL_NTC_HEAR_ISS_DT as DATE )                AS                                CDL_NTC_HEAR_ISS_DT 
		, cast( CDL_NTC_HEAR_RECV_DT as DATE )               AS                               CDL_NTC_HEAR_RECV_DT 
		, cast( CDL_NTC_APL_DUE_DT as DATE )                 AS                                 CDL_NTC_APL_DUE_DT 
		, cast( CDL_NTC_APL_FILE_DT as DATE )                AS                                CDL_NTC_APL_FILE_DT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CDL
            ),
LOGIC_CJT as ( SELECT 
		  upper( TRIM( CASE_JUR_TYP_NM ) )                   AS                                    CASE_JUR_TYP_NM 
		, upper( TRIM( CASE_JUR_TYP_CD ) )                   AS                                    CASE_JUR_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CJT
            ),
LOGIC_CVT as ( SELECT 
		  upper( TRIM( CASE_VENU_TYP_NM ) )                  AS                                   CASE_VENU_TYP_NM 
		, upper( TRIM( CASE_VENU_TYP_CD ) )                  AS                                   CASE_VENU_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CVT
            ),
LOGIC_CVLT as ( SELECT 
		  upper( TRIM( CASE_VENU_LOC_TYP_NM ) )              AS                               CASE_VENU_LOC_TYP_NM 
		, upper( TRIM( IC_CD ) )                             AS                                              IC_CD 
		, upper( TRIM( CASE_VENU_LOC_TYP_CD ) )              AS                               CASE_VENU_LOC_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CVLT
            )

---- RENAME LAYER ----
,

RENAME_CDL as ( SELECT 
		  CDL_ID                                             as                                             CDL_ID
		, CASE_ID                                            as                                            CASE_ID
		, CASE_JUR_TYP_CD                                    as                                    CASE_JUR_TYP_CD
		, CASE_VENU_TYP_CD                                   as                                   CASE_VENU_TYP_CD
		, CASE_VENU_LOC_TYP_CD                               as                               CASE_VENU_LOC_TYP_CD
		, CDL_NTC_HEAR_ISS_DT                                as                                CDL_NTC_HEAR_ISS_DT
		, CDL_NTC_HEAR_RECV_DT                               as                               CDL_NTC_HEAR_RECV_DT
		, CDL_NTC_APL_DUE_DT                                 as                                 CDL_NTC_APL_DUE_DT
		, CDL_NTC_APL_FILE_DT                                as                                CDL_NTC_APL_FILE_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CDL   ), 
RENAME_CJT as ( SELECT 
		  CASE_JUR_TYP_NM                                    as                                    CASE_JUR_TYP_NM
		, CASE_JUR_TYP_CD                                    as                                CJT_CASE_JUR_TYP_CD
		, VOID_IND                                           as                                       CJT_VOID_IND 
				FROM     LOGIC_CJT   ), 
RENAME_CVT as ( SELECT 
		  CASE_VENU_TYP_NM                                   as                                   CASE_VENU_TYP_NM
		, CASE_VENU_TYP_CD                                   as                               CVT_CASE_VENU_TYP_CD
		, VOID_IND                                           as                                       CVT_VOID_IND 
				FROM     LOGIC_CVT   ), 
RENAME_CVLT as ( SELECT 
		  CASE_VENU_LOC_TYP_NM                               as                               CASE_VENU_LOC_TYP_NM
		, IC_CD                                              as                            CASE_VENU_LOC_TYP_IC_CD
		, CASE_VENU_LOC_TYP_CD                               as                          CVLT_CASE_VENU_LOC_TYP_CD
		, VOID_IND                                           as                                      CVLT_VOID_IND 
				FROM     LOGIC_CVLT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CDL                            as ( SELECT * from    RENAME_CDL   ),
FILTER_CJT                            as ( SELECT * from    RENAME_CJT 
				WHERE CJT_VOID_IND = 'N'  ),
FILTER_CVT                            as ( SELECT * from    RENAME_CVT 
				WHERE CVT_VOID_IND = 'N'  ),
FILTER_CVLT                           as ( SELECT * from    RENAME_CVLT 
				WHERE CVLT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CDL as ( SELECT * 
				FROM  FILTER_CDL
				LEFT JOIN FILTER_CJT ON  FILTER_CDL.CASE_JUR_TYP_CD =  FILTER_CJT.CJT_CASE_JUR_TYP_CD 
						LEFT JOIN FILTER_CVT ON  FILTER_CDL.CASE_VENU_TYP_CD =  FILTER_CVT.CVT_CASE_VENU_TYP_CD 
						LEFT JOIN FILTER_CVLT ON  FILTER_CDL.CASE_VENU_LOC_TYP_CD =  FILTER_CVLT.CVLT_CASE_VENU_LOC_TYP_CD  )
SELECT * 
from CDL
      );
    