

      create or replace  table DEV_EDW.STAGING.STG_WC_CLASS_RATE_TIER  as
      (---- SRC LAYER ----
WITH
SRC_RT as ( SELECT *     from     DEV_VIEWS.PCMP.WC_CLASS_RATE_TIER ),
SRC_RTT as ( SELECT *     from     DEV_VIEWS.PCMP.WC_CLASS_RATE_TIER_TYPE ),
//SRC_RT as ( SELECT *     from     WC_CLASS_RATE_TIER) ,
//SRC_RTT as ( SELECT *     from     WC_CLASS_RATE_TIER_TYPE) ,

---- LOGIC LAYER ----

LOGIC_RT as ( SELECT 
		  WC_CLS_RT_TIER_ID                                  AS                                  WC_CLS_RT_TIER_ID 
		, WC_CLS_ID                                          AS                                          WC_CLS_ID 
		, upper( TRIM( WC_CLS_RT_TIER_TYP_CD ) )             AS                              WC_CLS_RT_TIER_TYP_CD 
		, cast( WC_CLS_RT_TIER_EFF_DT as DATE )              AS                              WC_CLS_RT_TIER_EFF_DT 
		, cast( WC_CLS_RT_TIER_END_DT as DATE )              AS                              WC_CLS_RT_TIER_END_DT 
		, WC_CLS_RT_TIER_RT                                  AS                                  WC_CLS_RT_TIER_RT 
		, WC_CLS_RT_TIER_PLMNRY_RT                           AS                           WC_CLS_RT_TIER_PLMNRY_RT 
		, WC_CLS_RT_TIER_MNM_PREM_AMT                        AS                        WC_CLS_RT_TIER_MNM_PREM_AMT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( WC_CLS_RT_TIER_VOID_IND )                   AS                            WC_CLS_RT_TIER_VOID_IND 
		from SRC_RT
            ),
LOGIC_RTT as ( SELECT 
		  upper( TRIM( WC_CLS_RT_TIER_TYP_NM ) )             AS                              WC_CLS_RT_TIER_TYP_NM 
		, upper( TRIM( WC_CLS_RT_TIER_TYP_CD ) )             AS                              WC_CLS_RT_TIER_TYP_CD 
		, upper( WC_CLS_RT_TIER_TYP_VOID_IND )               AS                        WC_CLS_RT_TIER_TYP_VOID_IND 
		from SRC_RTT
            )

---- RENAME LAYER ----
,

RENAME_RT as ( SELECT 
		  WC_CLS_RT_TIER_ID                                  as                                  WC_CLS_RT_TIER_ID
		, WC_CLS_ID                                          as                                          WC_CLS_ID
		, WC_CLS_RT_TIER_TYP_CD                              as                              WC_CLS_RT_TIER_TYP_CD
		, WC_CLS_RT_TIER_EFF_DT                              as                              WC_CLS_RT_TIER_EFF_DT
		, WC_CLS_RT_TIER_END_DT                              as                              WC_CLS_RT_TIER_END_DT
		, WC_CLS_RT_TIER_RT                                  as                                  WC_CLS_RT_TIER_RT
		, WC_CLS_RT_TIER_PLMNRY_RT                           as                           WC_CLS_RT_TIER_PLMNRY_RT
		, WC_CLS_RT_TIER_MNM_PREM_AMT                        as                        WC_CLS_RT_TIER_MNM_PREM_AMT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, WC_CLS_RT_TIER_VOID_IND                            as                            WC_CLS_RT_TIER_VOID_IND 
				FROM     LOGIC_RT   ), 
RENAME_RTT as ( SELECT 
		  WC_CLS_RT_TIER_TYP_NM                              as                              WC_CLS_RT_TIER_TYP_NM
		, WC_CLS_RT_TIER_TYP_CD                              as                          RTT_WC_CLS_RT_TIER_TYP_CD
		, WC_CLS_RT_TIER_TYP_VOID_IND                        as                        WC_CLS_RT_TIER_TYP_VOID_IND 
				FROM     LOGIC_RTT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_RT                             as ( SELECT * from    RENAME_RT   ),
FILTER_RTT                            as ( SELECT * from    RENAME_RTT 
				WHERE WC_CLS_RT_TIER_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

RT as ( SELECT * 
				FROM  FILTER_RT
				LEFT JOIN FILTER_RTT ON  FILTER_RT.WC_CLS_RT_TIER_TYP_CD =  FILTER_RTT.RTT_WC_CLS_RT_TIER_TYP_CD  )
SELECT * 
from RT
      );
    