

      create or replace  table DEV_EDW.STAGING.STG_POLICY_PERIOD_PREMIUM_DRV  as
      (---- SRC LAYER ----
WITH
SRC_PPPD as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD_PREMIUM_DRV ),
SRC_PT as ( SELECT *     from     DEV_VIEWS.PCMP.PREMIUM_TYPE ),
SRC_RENT as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_ELEMENT_NAME_TYPE ),
//SRC_PPPD as ( SELECT *     from     POLICY_PERIOD_PREMIUM_DRV) ,
//SRC_PT as ( SELECT *     from     PREMIUM_TYPE) ,
//SRC_RENT as ( SELECT *     from     RATING_ELEMENT_NAME_TYPE) ,

---- LOGIC LAYER ----

LOGIC_PPPD as ( SELECT 
		  PLCY_PRD_PREM_DRV_ID                               AS                               PLCY_PRD_PREM_DRV_ID 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, upper( TRIM( JUR_TYP_CD ) )                        AS                                         JUR_TYP_CD 
		, upper( TRIM( PREM_TYP_CD ) )                       AS                                        PREM_TYP_CD 
		, upper( TRIM( RT_ELEM_TYP_CD ) )                    AS                                     RT_ELEM_TYP_CD 
		, PLCY_PRD_PREM_DRV_NO                               AS                               PLCY_PRD_PREM_DRV_NO 
		, PLCY_PRD_PREM_DRV_SRT_NO                           AS                           PLCY_PRD_PREM_DRV_SRT_NO 
		, cast( PLCY_PRD_PREM_DRV_EFF_DTM as DATE )          AS                          PLCY_PRD_PREM_DRV_EFF_DTM 
		, cast( PLCY_PRD_PREM_DRV_END_DTM as DATE )          AS                          PLCY_PRD_PREM_DRV_END_DTM 
		, PLCY_PRD_PREM_DRV_PREM_AMT                         AS                         PLCY_PRD_PREM_DRV_PREM_AMT 
		, PLCY_PRD_PREM_DRV_ADJ_AMT                          AS                          PLCY_PRD_PREM_DRV_ADJ_AMT 
		, PLCY_PRD_PREM_DRV_ASMT_AMT                         AS                         PLCY_PRD_PREM_DRV_ASMT_AMT 
		, PLCY_PRD_PREM_DRV_RPT_ASMT_AMT                     AS                     PLCY_PRD_PREM_DRV_RPT_ASMT_AMT 
		, PLCY_PRD_PREM_DRV_MOD_VAL                          AS                          PLCY_PRD_PREM_DRV_MOD_VAL 
		, upper( TRIM( PLCY_PRD_PREM_DRV_STAT_CD ) )         AS                          PLCY_PRD_PREM_DRV_STAT_CD 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PPPD
            ),
LOGIC_PT as ( SELECT 
		  upper( TRIM( PREM_TYP_NM ) )                       AS                                        PREM_TYP_NM 
		, upper( TRIM( PREM_TYP_CD ) )                       AS                                        PREM_TYP_CD 
		from SRC_PT
            ),
LOGIC_RENT as ( SELECT 
		  upper( TRIM( RT_ELEM_TYP_NM ) )                    AS                                     RT_ELEM_TYP_NM 
		, upper( TRIM( RT_ELEM_TYP_CD ) )                    AS                                     RT_ELEM_TYP_CD 
		from SRC_RENT
            )

---- RENAME LAYER ----
,

RENAME_PPPD as ( SELECT 
		  PLCY_PRD_PREM_DRV_ID                               as                               PLCY_PRD_PREM_DRV_ID
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD
		, PREM_TYP_CD                                        as                                        PREM_TYP_CD
		, RT_ELEM_TYP_CD                                     as                                     RT_ELEM_TYP_CD
		, PLCY_PRD_PREM_DRV_NO                               as                               PLCY_PRD_PREM_DRV_NO
		, PLCY_PRD_PREM_DRV_SRT_NO                           as                           PLCY_PRD_PREM_DRV_SRT_NO
		, PLCY_PRD_PREM_DRV_EFF_DTM                          as                          PLCY_PRD_PREM_DRV_EFF_DTM
		, PLCY_PRD_PREM_DRV_END_DTM                          as                          PLCY_PRD_PREM_DRV_END_DTM
		, PLCY_PRD_PREM_DRV_PREM_AMT                         as                         PLCY_PRD_PREM_DRV_PREM_AMT
		, PLCY_PRD_PREM_DRV_ADJ_AMT                          as                          PLCY_PRD_PREM_DRV_ADJ_AMT
		, PLCY_PRD_PREM_DRV_ASMT_AMT                         as                         PLCY_PRD_PREM_DRV_ASMT_AMT
		, PLCY_PRD_PREM_DRV_RPT_ASMT_AMT                     as                     PLCY_PRD_PREM_DRV_RPT_ASMT_AMT
		, PLCY_PRD_PREM_DRV_MOD_VAL                          as                          PLCY_PRD_PREM_DRV_MOD_VAL
		, PLCY_PRD_PREM_DRV_STAT_CD                          as                          PLCY_PRD_PREM_DRV_STAT_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_PPPD   ), 
RENAME_PT as ( SELECT 
		  PREM_TYP_NM                                        as                                        PREM_TYP_NM
		, PREM_TYP_CD                                        as                                     PT_PREM_TYP_CD 
				FROM     LOGIC_PT   ), 
RENAME_RENT as ( SELECT 
		  RT_ELEM_TYP_NM                                     as                                     RT_ELEM_TYP_NM
		, RT_ELEM_TYP_CD                                     as                                RENT_RT_ELEM_TYP_CD 
				FROM     LOGIC_RENT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PPPD                           as ( SELECT * from    RENAME_PPPD   ),
FILTER_PT                             as ( SELECT * from    RENAME_PT   ),
FILTER_RENT                           as ( SELECT * from    RENAME_RENT   ),

---- JOIN LAYER ----

PPPD as ( SELECT * 
				FROM  FILTER_PPPD
				LEFT JOIN FILTER_PT ON  FILTER_PPPD.PREM_TYP_CD =  FILTER_PT.PT_PREM_TYP_CD 
								LEFT JOIN FILTER_RENT ON  FILTER_PPPD.RT_ELEM_TYP_CD =  FILTER_RENT.RENT_RT_ELEM_TYP_CD  )
SELECT * 
from PPPD
      );
    