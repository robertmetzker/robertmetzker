

      create or replace  table DEV_EDW.STAGING.STG_PAYROLL_REPORT  as
      (---- SRC LAYER ----
WITH
SRC_PR as ( SELECT *     from     DEV_VIEWS.PCMP.PAYROLL_REPORT ),
SRC_PRT as ( SELECT *     from     DEV_VIEWS.PCMP.PAYROLL_REPORT_TYPE ),
SRC_STS as ( SELECT *     from     DEV_VIEWS.PCMP.PAYROLL_REPORT_STATUS_TYPE ),
SRC_RSN as ( SELECT *     from     DEV_VIEWS.PCMP.PAYROLL_REPORT_STATUS_RSN_TYP ),
SRC_TRK as ( SELECT *     from     DEV_VIEWS.PCMP.PAYMENT_TRACK_SOURCE_TYPE ),
SRC_BPR as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_PAYROLL_REPORT ),
SRC_BPP as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_PAY_PLAN_OPTION_TYPE ),
//SRC_PR as ( SELECT *     from     PAYROLL_REPORT) ,
//SRC_PRT as ( SELECT *     from     PAYROLL_REPORT_TYPE) ,
//SRC_STS as ( SELECT *     from     PAYROLL_REPORT_STATUS_TYPE) ,
//SRC_RSN as ( SELECT *     from     PAYROLL_REPORT_STATUS_RSN_TYP) ,
//SRC_TRK as ( SELECT *     from     PAYMENT_TRACK_SOURCE_TYPE) ,
//SRC_BPR as ( SELECT *     from     BWC_PAYROLL_REPORT) ,
//SRC_BPP as ( SELECT *     from     BWC_PAY_PLAN_OPTION_TYPE) ,

---- LOGIC LAYER ----

LOGIC_PR as ( SELECT 
		  PYRL_RPT_ID                                        AS                                        PYRL_RPT_ID 
		, upper( TRIM( PYRL_RPT_TYP_CD ) )                   AS                                    PYRL_RPT_TYP_CD 
		, cast( PYRL_RPT_EFF_DT as DATE )                    AS                                    PYRL_RPT_EFF_DT 
		, cast( PYRL_RPT_END_DT as DATE )                    AS                                    PYRL_RPT_END_DT 
		, cast( PYRL_RPT_ISS_DT as DATE )                    AS                                    PYRL_RPT_ISS_DT 
		, cast( PYRL_RPT_DUE_DT as DATE )                    AS                                    PYRL_RPT_DUE_DT 
		, cast( PYRL_RPT_SCND_PAY_DUE_DT as DATE )           AS                           PYRL_RPT_SCND_PAY_DUE_DT 
		, cast( PYRL_RPT_RECV_DT as DATE )                   AS                                   PYRL_RPT_RECV_DT 
		, cast( PYRL_RPT_MOD_DT as DATE )                    AS                                    PYRL_RPT_MOD_DT 
		, upper( TRIM( PYRL_RPT_STS_TYP_CD ) )               AS                                PYRL_RPT_STS_TYP_CD 
		, upper( TRIM( PYRL_RPT_STS_RSN_TYP_CD ) )           AS                            PYRL_RPT_STS_RSN_TYP_CD 
		, upper( PYRL_RPT_EST_RPT_IND )                      AS                               PYRL_RPT_EST_RPT_IND 
--		, upper( PYRL_RPT_BTCH_SEL_IND )                     AS                              PYRL_RPT_BTCH_SEL_IND 
		, upper( PYRL_RPT_MIN_PYMT_RECV_IND )                AS                         PYRL_RPT_MIN_PYMT_RECV_IND 
		, upper( TRIM( PYRL_RPT_ENT_BY_USER_VAL ) )          AS                           PYRL_RPT_ENT_BY_USER_VAL 
		, upper( TRIM( PAY_TRK_SRC_TYP_CD ) )                AS                                 PAY_TRK_SRC_TYP_CD 
		, upper( TRIM( PYRL_RPT_PAY_TRK_SRC_TYP_NM ) )       AS                        PYRL_RPT_PAY_TRK_SRC_TYP_NM 
		, upper( PYRL_RPT_SBJ_TO_NO_COV_PEN_IND )            AS                     PYRL_RPT_SBJ_TO_NO_COV_PEN_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PR
            ),
LOGIC_PRT as ( SELECT 
		  upper( TRIM( PYRL_RPT_TYP_NM ) )                   AS                                    PYRL_RPT_TYP_NM 
		, upper( TRIM( PYRL_RPT_TYP_CD ) )                   AS                                    PYRL_RPT_TYP_CD 
		, upper( PYRL_RPT_TYP_VOID_IND )                     AS                              PYRL_RPT_TYP_VOID_IND 
		from SRC_PRT
            ),
LOGIC_STS as ( SELECT 
		  upper( TRIM( PYRL_RPT_STS_TYP_NM ) )               AS                                PYRL_RPT_STS_TYP_NM 
		, upper( TRIM( PYRL_RPT_STS_TYP_CD ) )               AS                                PYRL_RPT_STS_TYP_CD 
		, upper( PYRL_RPT_STS_TYP_VOID_IND )                 AS                          PYRL_RPT_STS_TYP_VOID_IND 
		from SRC_STS
            ),
LOGIC_RSN as ( SELECT 
		  upper( TRIM( PYRL_RPT_STS_RSN_TYP_NM ) )           AS                            PYRL_RPT_STS_RSN_TYP_NM 
		, upper( TRIM( PYRL_RPT_STS_RSN_TYP_CD ) )           AS                            PYRL_RPT_STS_RSN_TYP_CD 
		, upper( PYRL_RPT_STS_RSN_TYP_VOID_IND )             AS                      PYRL_RPT_STS_RSN_TYP_VOID_IND 
		from SRC_RSN
            ),
LOGIC_TRK as ( SELECT 
		  upper( TRIM( PAY_TRK_SRC_TYP_NM ) )                AS                                 PAY_TRK_SRC_TYP_NM 
		, upper( PAY_TRK_SRC_WEB_IND )                       AS                                PAY_TRK_SRC_WEB_IND 
		, upper( TRIM( PAY_TRK_SRC_TYP_CD ) )                AS                                 PAY_TRK_SRC_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_TRK
            ),
LOGIC_BPR as ( SELECT 
		  upper( TRIM( PAY_PLN_OPT_TYP_CD ) )                AS                                 PAY_PLN_OPT_TYP_CD 
		, PYRL_RPT_ID                                        AS                                        PYRL_RPT_ID 
		from SRC_BPR
            ),
LOGIC_BPP as ( SELECT 
		  upper( TRIM( PAY_PLN_OPT_TYP_NM ) )                AS                                 PAY_PLN_OPT_TYP_NM 
		, PAY_PLN_OPT_TYP_FST_PAY_PCT                        AS                        PAY_PLN_OPT_TYP_FST_PAY_PCT 
		, PAY_PLN_OPT_TYP_SCND_PAY_PCT                       AS                       PAY_PLN_OPT_TYP_SCND_PAY_PCT 
		, upper( TRIM( PAY_PLN_OPT_TYP_CD ) )                AS                                 PAY_PLN_OPT_TYP_CD 
		from SRC_BPP
            )

---- RENAME LAYER ----
,

RENAME_PR as ( SELECT 
		  PYRL_RPT_ID                                        as                                        PYRL_RPT_ID
		, PYRL_RPT_TYP_CD                                    as                                    PYRL_RPT_TYP_CD
		, PYRL_RPT_EFF_DT                                    as                                    PYRL_RPT_EFF_DT
		, PYRL_RPT_END_DT                                    as                                    PYRL_RPT_END_DT
		, PYRL_RPT_ISS_DT                                    as                                    PYRL_RPT_ISS_DT
		, PYRL_RPT_DUE_DT                                    as                                    PYRL_RPT_DUE_DT
		, PYRL_RPT_SCND_PAY_DUE_DT                           as                           PYRL_RPT_SCND_PAY_DUE_DT
		, PYRL_RPT_RECV_DT                                   as                                   PYRL_RPT_RECV_DT
		, PYRL_RPT_MOD_DT                                    as                                    PYRL_RPT_MOD_DT
		, PYRL_RPT_STS_TYP_CD                                as                                PYRL_RPT_STS_TYP_CD
		, PYRL_RPT_STS_RSN_TYP_CD                            as                            PYRL_RPT_STS_RSN_TYP_CD
		, PYRL_RPT_EST_RPT_IND                               as                               PYRL_RPT_EST_RPT_IND
--		, PYRL_RPT_BTCH_SEL_IND                              as                              PYRL_RPT_BTCH_SEL_IND
		, PYRL_RPT_MIN_PYMT_RECV_IND                         as                         PYRL_RPT_MIN_PYMT_RECV_IND
		, PYRL_RPT_ENT_BY_USER_VAL                           as                           PYRL_RPT_ENT_BY_USER_VAL
		, PAY_TRK_SRC_TYP_CD                                 as                                 PAY_TRK_SRC_TYP_CD
		, PYRL_RPT_PAY_TRK_SRC_TYP_NM                        as                        PYRL_RPT_PAY_TRK_SRC_TYP_NM
		, PYRL_RPT_SBJ_TO_NO_COV_PEN_IND                     as                     PYRL_RPT_SBJ_TO_NO_COV_PEN_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_PR   ), 
RENAME_PRT as ( SELECT 
		  PYRL_RPT_TYP_NM                                    as                                    PYRL_RPT_TYP_NM
		, PYRL_RPT_TYP_CD                                    as                                PRT_PYRL_RPT_TYP_CD
		, PYRL_RPT_TYP_VOID_IND                              as                              PYRL_RPT_TYP_VOID_IND 
				FROM     LOGIC_PRT   ), 
RENAME_STS as ( SELECT 
		  PYRL_RPT_STS_TYP_NM                                as                                PYRL_RPT_STS_TYP_NM
		, PYRL_RPT_STS_TYP_CD                                as                            STS_PYRL_RPT_STS_TYP_CD
		, PYRL_RPT_STS_TYP_VOID_IND                          as                          PYRL_RPT_STS_TYP_VOID_IND 
				FROM     LOGIC_STS   ), 
RENAME_RSN as ( SELECT 
		  PYRL_RPT_STS_RSN_TYP_NM                            as                            PYRL_RPT_STS_RSN_TYP_NM
		, PYRL_RPT_STS_RSN_TYP_CD                            as                        RSN_PYRL_RPT_STS_RSN_TYP_CD
		, PYRL_RPT_STS_RSN_TYP_VOID_IND                      as                      PYRL_RPT_STS_RSN_TYP_VOID_IND 
				FROM     LOGIC_RSN   ), 
RENAME_TRK as ( SELECT 
		  PAY_TRK_SRC_TYP_NM                                 as                                 PAY_TRK_SRC_TYP_NM
		, PAY_TRK_SRC_WEB_IND                                as                                PAY_TRK_SRC_WEB_IND
		, PAY_TRK_SRC_TYP_CD                                 as                             TRK_PAY_TRK_SRC_TYP_CD
		, VOID_IND                                           as                                       TRK_VOID_IND 
				FROM     LOGIC_TRK   ), 
RENAME_BPR as ( SELECT 
		  PAY_PLN_OPT_TYP_CD                                 as                                 PAY_PLN_OPT_TYP_CD
		, PYRL_RPT_ID                                        as                                    BPR_PYRL_RPT_ID 
				FROM     LOGIC_BPR   ), 
RENAME_BPP as ( SELECT 
		  PAY_PLN_OPT_TYP_NM                                 as                                 PAY_PLN_OPT_TYP_NM
		, PAY_PLN_OPT_TYP_FST_PAY_PCT                        as                        PAY_PLN_OPT_TYP_FST_PAY_PCT
		, PAY_PLN_OPT_TYP_SCND_PAY_PCT                       as                       PAY_PLN_OPT_TYP_SCND_PAY_PCT
		, PAY_PLN_OPT_TYP_CD                                 as                             BPP_PAY_PLN_OPT_TYP_CD 
				FROM     LOGIC_BPP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PR                             as ( SELECT * from    RENAME_PR   ),
FILTER_PRT                            as ( SELECT * from    RENAME_PRT 
				WHERE PYRL_RPT_TYP_VOID_IND = 'N'  ),
FILTER_STS                            as ( SELECT * from    RENAME_STS 
				WHERE PYRL_RPT_STS_TYP_VOID_IND = 'N'  ),
FILTER_RSN                            as ( SELECT * from    RENAME_RSN 
				WHERE PYRL_RPT_STS_RSN_TYP_VOID_IND = 'N'  ),
FILTER_TRK                            as ( SELECT * from    RENAME_TRK 
				WHERE TRK_VOID_IND = 'N'  ),
FILTER_BPR                            as ( SELECT * from    RENAME_BPR   ),
FILTER_BPP                            as ( SELECT * from    RENAME_BPP   ),

---- JOIN LAYER ----

BPR as ( SELECT * 
				FROM  FILTER_BPR
				LEFT JOIN FILTER_BPP ON  FILTER_BPR.PAY_PLN_OPT_TYP_CD =  FILTER_BPP.BPP_PAY_PLN_OPT_TYP_CD  ),
PR as ( SELECT * 
				FROM  FILTER_PR
				LEFT JOIN FILTER_PRT ON  FILTER_PR.PYRL_RPT_TYP_CD =  FILTER_PRT.PRT_PYRL_RPT_TYP_CD 
								LEFT JOIN FILTER_STS ON  FILTER_PR.PYRL_RPT_STS_TYP_CD =  FILTER_STS.STS_PYRL_RPT_STS_TYP_CD 
								LEFT JOIN FILTER_RSN ON  FILTER_PR.PYRL_RPT_STS_RSN_TYP_CD =  FILTER_RSN.RSN_PYRL_RPT_STS_RSN_TYP_CD 
								LEFT JOIN FILTER_TRK ON  FILTER_PR.PAY_TRK_SRC_TYP_CD =  FILTER_TRK.TRK_PAY_TRK_SRC_TYP_CD 
						LEFT JOIN BPR ON  FILTER_PR.PYRL_RPT_ID = BPR.BPR_PYRL_RPT_ID  )
SELECT * 
from PR
      );
    