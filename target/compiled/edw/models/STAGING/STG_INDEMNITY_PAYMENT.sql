---- SRC LAYER ----
WITH
SRC_IP as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_PAYMENT ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
SRC_BT as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_TYPE ),
SRC_BCTG as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_CATEGORY_TYPE ),
SRC_BCYC as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_CYCLE_TYPE ),
SRC_BRT as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_REPORTING_TYPE ),
SRC_JT as ( SELECT *     from     DEV_VIEWS.PCMP.JURISDICTION_TYPE ),
SRC_IRT as ( SELECT *     from     DEV_VIEWS.PCMP.INDEMNITY_REASON_TYPE ),
//SRC_IP as ( SELECT *     from     INDEMNITY_PAYMENT) ,
//SRC_C as ( SELECT *     from     CLAIM) ,
//SRC_BT as ( SELECT *     from     BENEFIT_TYPE) ,
//SRC_BCTG as ( SELECT *     from     BENEFIT_CATEGORY_TYPE) ,
//SRC_BCYC as ( SELECT *     from     BENEFIT_CYCLE_TYPE) ,
//SRC_BRT as ( SELECT *     from     BENEFIT_REPORTING_TYPE) ,
//SRC_JT as ( SELECT *     from     JURISDICTION_TYPE) ,
//SRC_IRT as ( SELECT *     from     INDEMNITY_REASON_TYPE) ,

---- LOGIC LAYER ----

LOGIC_IP as ( SELECT 
		  INDM_PAY_ID                                        AS                                        INDM_PAY_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, cast( INDM_PAY_EFF_DT as DATE )                    AS                                    INDM_PAY_EFF_DT 
		, cast( INDM_PAY_END_DT as DATE )                    AS                                    INDM_PAY_END_DT 
		, upper( TRIM( BNFT_TYP_CD ) )                       AS                                        BNFT_TYP_CD 
		, BNFT_RPT_TYP_ID                                    AS                                    BNFT_RPT_TYP_ID 
		, upper( TRIM( INDM_RSN_TYP_CD ) )                   AS                                    INDM_RSN_TYP_CD 
		, INDM_PAY_DRV_WEK                                   AS                                   INDM_PAY_DRV_WEK 
		, INDM_PAY_DRV_DD                                    AS                                    INDM_PAY_DRV_DD 
		, INDM_PAY_BNFT_RT_AMT                               AS                               INDM_PAY_BNFT_RT_AMT 
		, INDM_PAY_CALC_BNFT_RT_AMT                          AS                          INDM_PAY_CALC_BNFT_RT_AMT 
		, INDM_PAY_OVRD_BNFT_RT_AMT                          AS                          INDM_PAY_OVRD_BNFT_RT_AMT 
		, INDM_PAY_DRV_TOT_AMT                               AS                               INDM_PAY_DRV_TOT_AMT 
		, INDM_PAY_DRV_SCH_TOT_AMT                           AS                           INDM_PAY_DRV_SCH_TOT_AMT 
		, INDM_PAY_OVRD_POST_INJR_AMT                        AS                        INDM_PAY_OVRD_POST_INJR_AMT 
		, upper( TRIM( INDM_PAY_OVRD_BNFT_RT_CMT ) )         AS                          INDM_PAY_OVRD_BNFT_RT_CMT 
		, upper( TRIM( INDM_PAY_OVRD_POST_INJR_COMT ) )      AS                       INDM_PAY_OVRD_POST_INJR_COMT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		, CLM_AVG_WG_ID                                      AS                                      CLM_AVG_WG_ID 
		, CLM_EARN_CAP_ID                                    AS                                    CLM_EARN_CAP_ID 
		, upper( INDM_PAY_RECALC_IND )                       AS                                INDM_PAY_RECALC_IND 
		, upper( INDM_PAY_NOTE_IND )                         AS                                  INDM_PAY_NOTE_IND 
		, upper( INDM_PAY_DO_NOT_UPDT_IND )                  AS                           INDM_PAY_DO_NOT_UPDT_IND
		, CALC_RSLT_ID                                       as                                       CALC_RSLT_ID 
		from SRC_IP
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CLM_NO ) )                            AS                                             CLM_NO 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( CLM_REL_SNPSHT_IND )                        AS                                 CLM_REL_SNPSHT_IND 
		from SRC_C
            ),
LOGIC_BT as ( SELECT 
		  upper( TRIM( BNFT_TYP_NM ) )                       AS                                        BNFT_TYP_NM 
		, upper( TRIM( BNFT_CTG_TYP_CD ) )                   AS                                    BNFT_CTG_TYP_CD 
		, upper( TRIM( BNFT_CYC_TYP_CD ) )                   AS                                    BNFT_CYC_TYP_CD 
		, upper( TRIM( BNFT_TYP_CD ) )                       AS                                        BNFT_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BT
            ),
LOGIC_BCTG as ( SELECT 
		  upper( TRIM( BNFT_CTG_TYP_NM ) )                   AS                                    BNFT_CTG_TYP_NM 
		, upper( TRIM( BNFT_CTG_TYP_CD ) )                   AS                                    BNFT_CTG_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BCTG
            ),
LOGIC_BCYC as ( SELECT 
		  upper( TRIM( BNFT_CYC_TYP_NM ) )                   AS                                    BNFT_CYC_TYP_NM 
		, upper( TRIM( BNFT_CYC_TYP_CD ) )                   AS                                    BNFT_CYC_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BCYC
            ),
LOGIC_BRT as ( SELECT 
		  upper( TRIM( BNFT_RPT_TYP_CD ) )                   AS                                    BNFT_RPT_TYP_CD 
		, upper( TRIM( BNFT_RPT_TYP_NM ) )                   AS                                    BNFT_RPT_TYP_NM 
		, upper( TRIM( JUR_TYP_CD ) )                        AS                                         JUR_TYP_CD 
		, BNFT_RPT_TYP_ID                                    AS                                    BNFT_RPT_TYP_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BRT
            ),
LOGIC_JT as ( SELECT 
		  upper( TRIM( JUR_TYP_NM ) )                        AS                                         JUR_TYP_NM 
		, upper( TRIM( JUR_TYP_CD ) )                        AS                                         JUR_TYP_CD 
		, upper( JUR_TYP_VOID_IND )                          AS                                   JUR_TYP_VOID_IND 
		from SRC_JT
            ),
LOGIC_IRT as ( SELECT 
		  upper( TRIM( INDM_RSN_TYP_NM ) )                   AS                                    INDM_RSN_TYP_NM 
		, upper( TRIM( INDM_RSN_TYP_CD ) )                   AS                                    INDM_RSN_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_IRT
            )

---- RENAME LAYER ----
,

RENAME_IP as ( SELECT 
		  INDM_PAY_ID                                        as                                        INDM_PAY_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, INDM_PAY_EFF_DT                                    as                                    INDM_PAY_EFF_DT
		, INDM_PAY_END_DT                                    as                                    INDM_PAY_END_DT
		, BNFT_TYP_CD                                        as                                        BNFT_TYP_CD
		, BNFT_RPT_TYP_ID                                    as                                    BNFT_RPT_TYP_ID
		, INDM_RSN_TYP_CD                                    as                                    INDM_RSN_TYP_CD
		, INDM_PAY_DRV_WEK                                   as                                   INDM_PAY_DRV_WEK
		, INDM_PAY_DRV_DD                                    as                                    INDM_PAY_DRV_DD
		, INDM_PAY_BNFT_RT_AMT                               as                               INDM_PAY_BNFT_RT_AMT
		, INDM_PAY_CALC_BNFT_RT_AMT                          as                          INDM_PAY_CALC_BNFT_RT_AMT
		, INDM_PAY_OVRD_BNFT_RT_AMT                          as                          INDM_PAY_OVRD_BNFT_RT_AMT
		, INDM_PAY_DRV_TOT_AMT                               as                               INDM_PAY_DRV_TOT_AMT
		, INDM_PAY_DRV_SCH_TOT_AMT                           as                           INDM_PAY_DRV_SCH_TOT_AMT
		, INDM_PAY_OVRD_POST_INJR_AMT                        as                        INDM_PAY_OVRD_POST_INJR_AMT
		, INDM_PAY_OVRD_BNFT_RT_CMT                          as                          INDM_PAY_OVRD_BNFT_RT_CMT
		, INDM_PAY_OVRD_POST_INJR_COMT                       as                       INDM_PAY_OVRD_POST_INJR_COMT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, CLM_AVG_WG_ID                                      as                                      CLM_AVG_WG_ID
		, CLM_EARN_CAP_ID                                    as                                    CLM_EARN_CAP_ID
		, INDM_PAY_RECALC_IND                                as                                INDM_PAY_RECALC_IND
		, INDM_PAY_NOTE_IND                                  as                                  INDM_PAY_NOTE_IND
		, INDM_PAY_DO_NOT_UPDT_IND                           as                           INDM_PAY_DO_NOT_UPDT_IND
		, CALC_RSLT_ID                                       as                                       CALC_RSLT_ID 
				FROM     LOGIC_IP   ), 
RENAME_C as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                          C_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
				FROM     LOGIC_C   ), 
RENAME_BT as ( SELECT 
		  BNFT_TYP_NM                                        as                                        BNFT_TYP_NM
		, BNFT_CTG_TYP_CD                                    as                                    BNFT_CTG_TYP_CD
		, BNFT_CYC_TYP_CD                                    as                                    BNFT_CYC_TYP_CD
		, BNFT_TYP_CD                                        as                                     BT_BNFT_TYP_CD
		, VOID_IND                                           as                                        BT_VOID_IND 
				FROM     LOGIC_BT   ), 
RENAME_BCTG as ( SELECT 
		  BNFT_CTG_TYP_NM                                    as                                    BNFT_CTG_TYP_NM
		, BNFT_CTG_TYP_CD                                    as                               BCTG_BNFT_CTG_TYP_CD
		, VOID_IND                                           as                                      BCTG_VOID_IND 
				FROM     LOGIC_BCTG   ), 
RENAME_BCYC as ( SELECT 
		  BNFT_CYC_TYP_NM                                    as                                    BNFT_CYC_TYP_NM
		, BNFT_CYC_TYP_CD                                    as                               BCYC_BNFT_CYC_TYP_CD
		, VOID_IND                                           as                                      BCYC_VOID_IND 
				FROM     LOGIC_BCYC   ), 
RENAME_BRT as ( SELECT 
		  BNFT_RPT_TYP_CD                                    as                                    BNFT_RPT_TYP_CD
		, BNFT_RPT_TYP_NM                                    as                                    BNFT_RPT_TYP_NM
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD
		, BNFT_RPT_TYP_ID                                    as                                BRT_BNFT_RPT_TYP_ID
		, VOID_IND                                           as                                       BRT_VOID_IND 
				FROM     LOGIC_BRT   ), 
RENAME_JT as ( SELECT 
		  JUR_TYP_NM                                         as                                         JUR_TYP_NM
		, JUR_TYP_CD                                         as                                      JT_JUR_TYP_CD
		, JUR_TYP_VOID_IND                                   as                                   JUR_TYP_VOID_IND 
				FROM     LOGIC_JT   ), 
RENAME_IRT as ( SELECT 
		  INDM_RSN_TYP_NM                                    as                                    INDM_RSN_TYP_NM
		, INDM_RSN_TYP_CD                                    as                                IRT_INDM_RSN_TYP_CD
		, VOID_IND                                           as                                       IRT_VOID_IND 
				FROM     LOGIC_IRT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_IP                             as ( SELECT * from    RENAME_IP   ),
FILTER_C                              as ( SELECT * from    RENAME_C 
				WHERE CLM_REL_SNPSHT_IND = 'N'  ),
FILTER_BT                             as ( SELECT * from    RENAME_BT 
				WHERE BT_VOID_IND = 'N'  ),
FILTER_BRT                            as ( SELECT * from    RENAME_BRT 
				WHERE BRT_VOID_IND = 'N'  ),
FILTER_IRT                            as ( SELECT * from    RENAME_IRT 
				WHERE IRT_VOID_IND = 'N'  ),
FILTER_BCTG                           as ( SELECT * from    RENAME_BCTG 
				WHERE BCTG_VOID_IND = 'N'  ),
FILTER_BCYC                           as ( SELECT * from    RENAME_BCYC 
				WHERE BCYC_VOID_IND = 'N'  ),
FILTER_JT                             as ( SELECT * from    RENAME_JT 
				WHERE JUR_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

BT as ( SELECT * 
				FROM  FILTER_BT
				LEFT JOIN FILTER_BCTG ON  FILTER_BT.BNFT_CTG_TYP_CD =  FILTER_BCTG.BCTG_BNFT_CTG_TYP_CD 
						LEFT JOIN FILTER_BCYC ON  FILTER_BT.BNFT_CYC_TYP_CD =  FILTER_BCYC.BCYC_BNFT_CYC_TYP_CD  ),
BRT as ( SELECT * 
				FROM  FILTER_BRT
				LEFT JOIN FILTER_JT ON  FILTER_BRT.JUR_TYP_CD =  FILTER_JT.JT_JUR_TYP_CD  ),
IP as ( SELECT * 
				FROM  FILTER_IP
				INNER JOIN FILTER_C ON  FILTER_IP.AGRE_ID =  FILTER_C.C_AGRE_ID 
						LEFT JOIN BT ON  FILTER_IP.BNFT_TYP_CD = BT.BT_BNFT_TYP_CD 
						LEFT JOIN BRT ON  FILTER_IP.BNFT_RPT_TYP_ID = BRT.BRT_BNFT_RPT_TYP_ID 
						LEFT JOIN FILTER_IRT ON  FILTER_IP.INDM_RSN_TYP_CD =  FILTER_IRT.IRT_INDM_RSN_TYP_CD  )
SELECT * 
from IP