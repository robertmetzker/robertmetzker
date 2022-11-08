

      create or replace  table DEV_EDW.STAGING.STG_WC_COVERAGE_PREMIUM  as
      (---- SRC LAYER ----
WITH
SRC_WCP as ( SELECT *     from     DEV_VIEWS.PCMP.WC_COVERAGE_PREMIUM ),
SRC_RT as ( SELECT *     from     DEV_VIEWS.PCMP.RATING_TYPE ),
SRC_BWCP as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_WC_COVERAGE_PREMIUM ),
SRC_PP             as ( SELECT *     from     STAGING.STG_PREMIUM_PERIOD ),
//SRC_WCP as ( SELECT *     from     WC_COVERAGE_PREMIUM) ,
//SRC_RT as ( SELECT *     from     RATING_TYPE) ,
//SRC_BWCP as ( SELECT *     from     BWC_WC_COVERAGE_PREMIUM) ,
//SRC_PP             as ( SELECT *     FROM     STG_PREMIUM_PERIOD) ,

---- LOGIC LAYER ----

LOGIC_WCP as ( SELECT 
		  WC_COV_PREM_ID                                     AS                                     WC_COV_PREM_ID 
		, PREM_PRD_ID                                        AS                                        PREM_PRD_ID 
		, RISK_ID                                            AS                                            RISK_ID 
		, WC_CLS_SUFX_ID                                     AS                                     WC_CLS_SUFX_ID 
		, CUST_ID_PTCP_BUSN_INS                              AS                              CUST_ID_PTCP_BUSN_INS 
		, CUST_ID_COV                                        AS                                        CUST_ID_COV 
		, PTCP_ID_COV                                        AS                                        PTCP_ID_COV 
		, WC_COV_PREM_NO                                     AS                                     WC_COV_PREM_NO 
		, upper( TRIM( RT_TYP_CD ) )                         AS                                          RT_TYP_CD 
		, PYRL_RPT_ID                                        AS                                        PYRL_RPT_ID 
		, cast( WC_COV_PREM_EFF_DT as DATE )                 AS                                 WC_COV_PREM_EFF_DT 
		, cast( WC_COV_PREM_END_DT as DATE )                 AS                                 WC_COV_PREM_END_DT 
		, upper( WC_COV_PREM_RN_IND )                        AS                                 WC_COV_PREM_RN_IND 
		, WC_COV_PREM_BS_VAL                                 AS                                 WC_COV_PREM_BS_VAL 
		, WC_COV_PREM_BS_UNT                                 AS                                 WC_COV_PREM_BS_UNT 
		, WC_COV_PREM_BS_VAL_ADJ                             AS                             WC_COV_PREM_BS_VAL_ADJ 
		, WC_COV_PREM_BS_UNT_ADJ                             AS                             WC_COV_PREM_BS_UNT_ADJ 
		, WC_COV_PREM_DRV_MNL_PREM_AMT                       AS                       WC_COV_PREM_DRV_MNL_PREM_AMT 
		, WC_COV_PREM_DRV_MNL_PREM_A_A                       AS                       WC_COV_PREM_DRV_MNL_PREM_A_A 
		, WC_COV_PREM_DRV_MNM_PREM_AMT                       AS                       WC_COV_PREM_DRV_MNM_PREM_AMT 
		, WC_COV_PREM_DRV_RT                                 AS                                 WC_COV_PREM_DRV_RT 
		, WC_COV_PREM_DRV_CNSN_TO_RT                         AS                         WC_COV_PREM_DRV_CNSN_TO_RT 
		, upper( WC_COV_PREM_COMPN_COV_IND )                 AS                          WC_COV_PREM_COMPN_COV_IND 
		, upper( WC_COV_PREM_RT_OVRRD_IND )                  AS                           WC_COV_PREM_RT_OVRRD_IND 
		, WC_COV_PREM_TOT_PYRL                               AS                               WC_COV_PREM_TOT_PYRL 
		, WC_COV_PREM_CMRC_PYRL_PCT                          AS                          WC_COV_PREM_CMRC_PYRL_PCT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( WC_COV_PREM_VOID_IND )                      AS                               WC_COV_PREM_VOID_IND 
		from SRC_WCP
            ),
LOGIC_RT as ( SELECT 
		  upper( TRIM( RT_TYP_NM ) )                         AS                                          RT_TYP_NM 
		, upper( TRIM( RT_TYP_CD ) )                         AS                                          RT_TYP_CD 
		, upper( RT_TYP_VOID_IND )                           AS                                    RT_TYP_VOID_IND 
		from SRC_RT
            ),
LOGIC_BWCP as ( SELECT 
		  WC_COV_PREM_PURE_RT                                AS                                WC_COV_PREM_PURE_RT 
		, WC_COV_PREM_PURE_PREM_AMT                          AS                          WC_COV_PREM_PURE_PREM_AMT 
		, WC_COV_PREM_BASE_RT                                AS                                WC_COV_PREM_BASE_RT 
		, WC_COV_PREM_BWC_ADMN_RT                            AS                            WC_COV_PREM_BWC_ADMN_RT 
		, WC_COV_PREM_IC_ADMN_RT                             AS                             WC_COV_PREM_IC_ADMN_RT 
		, WC_COV_PREM_DWRF_RT                                AS                                WC_COV_PREM_DWRF_RT 
		, WC_COV_PREM_DWRF_II_RT                             AS                             WC_COV_PREM_DWRF_II_RT 
		, WC_COV_PREM_PD_PURE_PREM_AMT                       AS                       WC_COV_PREM_PD_PURE_PREM_AMT 
		, WC_COV_PREM_PD_PURE_PREM_RT                        AS                        WC_COV_PREM_PD_PURE_PREM_RT 
		, WC_COV_PREM_ID                                     AS                                     WC_COV_PREM_ID 
		from SRC_BWCP
            ),

LOGIC_PP as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, PREM_PRD_ID                                        as                                        PREM_PRD_ID 
		FROM SRC_PP
            )

---- RENAME LAYER ----
,

RENAME_WCP as ( SELECT 
		  WC_COV_PREM_ID                                     as                                     WC_COV_PREM_ID
		, PREM_PRD_ID                                        as                                        PREM_PRD_ID
		, RISK_ID                                            as                                            RISK_ID
		, WC_CLS_SUFX_ID                                     as                                     WC_CLS_SUFX_ID
		, CUST_ID_PTCP_BUSN_INS                              as                              CUST_ID_PTCP_BUSN_INS
		, CUST_ID_COV                                        as                                        CUST_ID_COV
		, PTCP_ID_COV                                        as                                        PTCP_ID_COV
		, WC_COV_PREM_NO                                     as                                     WC_COV_PREM_NO
		, RT_TYP_CD                                          as                                          RT_TYP_CD
		, PYRL_RPT_ID                                        as                                        PYRL_RPT_ID
		, WC_COV_PREM_EFF_DT                                 as                                 WC_COV_PREM_EFF_DT
		, WC_COV_PREM_END_DT                                 as                                 WC_COV_PREM_END_DT
		, WC_COV_PREM_RN_IND                                 as                                 WC_COV_PREM_RN_IND
		, WC_COV_PREM_BS_VAL                                 as                                 WC_COV_PREM_BS_VAL
		, WC_COV_PREM_BS_UNT                                 as                                 WC_COV_PREM_BS_UNT
		, WC_COV_PREM_BS_VAL_ADJ                             as                             WC_COV_PREM_BS_VAL_ADJ
		, WC_COV_PREM_BS_UNT_ADJ                             as                             WC_COV_PREM_BS_UNT_ADJ
		, WC_COV_PREM_DRV_MNL_PREM_AMT                       as                       WC_COV_PREM_DRV_MNL_PREM_AMT
		, WC_COV_PREM_DRV_MNL_PREM_A_A                       as                       WC_COV_PREM_DRV_MNL_PREM_A_A
		, WC_COV_PREM_DRV_MNM_PREM_AMT                       as                       WC_COV_PREM_DRV_MNM_PREM_AMT
		, WC_COV_PREM_DRV_RT                                 as                                 WC_COV_PREM_DRV_RT
		, WC_COV_PREM_DRV_CNSN_TO_RT                         as                         WC_COV_PREM_DRV_CNSN_TO_RT
		, WC_COV_PREM_COMPN_COV_IND                          as                          WC_COV_PREM_COMPN_COV_IND
		, WC_COV_PREM_RT_OVRRD_IND                           as                           WC_COV_PREM_RT_OVRRD_IND
		, WC_COV_PREM_TOT_PYRL                               as                               WC_COV_PREM_TOT_PYRL
		, WC_COV_PREM_CMRC_PYRL_PCT                          as                          WC_COV_PREM_CMRC_PYRL_PCT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, WC_COV_PREM_VOID_IND                               as                               WC_COV_PREM_VOID_IND 
				FROM     LOGIC_WCP   ), 
RENAME_RT as ( SELECT 
		  RT_TYP_NM                                          as                                          RT_TYP_NM
		, RT_TYP_CD                                          as                                       RT_RT_TYP_CD
		, RT_TYP_VOID_IND                                    as                                    RT_TYP_VOID_IND 
				FROM     LOGIC_RT   ), 
RENAME_BWCP as ( SELECT 
		  WC_COV_PREM_PURE_RT                                as                                WC_COV_PREM_PURE_RT
		, WC_COV_PREM_PURE_PREM_AMT                          as                          WC_COV_PREM_PURE_PREM_AMT
		, WC_COV_PREM_BASE_RT                                as                                WC_COV_PREM_BASE_RT
		, WC_COV_PREM_BWC_ADMN_RT                            as                            WC_COV_PREM_BWC_ADMN_RT
		, WC_COV_PREM_IC_ADMN_RT                             as                             WC_COV_PREM_IC_ADMN_RT
		, WC_COV_PREM_DWRF_RT                                as                                WC_COV_PREM_DWRF_RT
		, WC_COV_PREM_DWRF_II_RT                             as                             WC_COV_PREM_DWRF_II_RT
		, WC_COV_PREM_PD_PURE_PREM_AMT                       as                       WC_COV_PREM_PD_PURE_PREM_AMT
		, WC_COV_PREM_PD_PURE_PREM_RT                        as                        WC_COV_PREM_PD_PURE_PREM_RT
		, WC_COV_PREM_ID                                     as                                BWCP_WC_COV_PREM_ID 
				FROM     LOGIC_BWCP   ), 
RENAME_PP         as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PREM_PRD_ID                                        as                                     PP_PREM_PRD_ID 
				FROM     LOGIC_PP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_WCP                            as ( SELECT * from    RENAME_WCP   ),
FILTER_BWCP                           as ( SELECT * from    RENAME_BWCP   ),
FILTER_RT                             as ( SELECT * from    RENAME_RT 
				WHERE RT_TYP_VOID_IND = 'N'  ),
FILTER_PP                             as ( SELECT * FROM    RENAME_PP   ),

---- JOIN LAYER ----

WCP as ( SELECT * 
				FROM  FILTER_WCP
				LEFT JOIN FILTER_BWCP ON  FILTER_WCP.WC_COV_PREM_ID =  FILTER_BWCP.BWCP_WC_COV_PREM_ID 
								LEFT JOIN FILTER_RT ON  FILTER_WCP.RT_TYP_CD =  FILTER_RT.RT_RT_TYP_CD 
								LEFT JOIN FILTER_pp ON  FILTER_WCP.PREM_PRD_ID =  FILTER_PP.PP_PREM_PRD_ID  )
SELECT * 
from WCP
      );
    