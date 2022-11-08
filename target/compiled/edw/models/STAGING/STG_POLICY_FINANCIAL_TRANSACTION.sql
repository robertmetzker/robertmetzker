---- SRC LAYER ----
WITH
SRC_PFT as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_FINANCIAL_TRANSACTION ),
SRC_FTT as ( SELECT *     from     DEV_VIEWS.PCMP.FINANCIAL_TRANSACTION_TYPE ),
SRC_PTST as ( SELECT *     from     DEV_VIEWS.PCMP.PAYMENT_TRACK_SOURCE_TYPE ),
SRC_BPFT as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_POLICY_FINANCIAL_TRAN ),
SRC_BFTST as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_FINANCIAL_TRAN_SUB_TYP ),
SRC_PP as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD ),
SRC_PP2 as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD ),
//SRC_PFT as ( SELECT *     from     PCMP.POLICY_FINANCIAL_TRANSACTION) ,
//SRC_FTT as ( SELECT *     from     PCMP.FINANCIAL_TRANSACTION_TYPE) ,
//SRC_PTST as ( SELECT *     from     PCMP.PAYMENT_TRACK_SOURCE_TYPE) ,
//SRC_BPFT as ( SELECT *     from     PCMP.BWC_POLICY_FINANCIAL_TRAN) ,
//SRC_BFTST as ( SELECT *     from     PCMP.BWC_FINANCIAL_TRAN_SUB_TYP) ,
//SRC_PP as ( SELECT *     from     PCMP.POLICY_PERIOD) ,
//SRC_PP2 as ( SELECT *     from     PCMP.POLICY_PERIOD) ,

---- LOGIC LAYER ----

LOGIC_PFT as ( SELECT 
		  PFT_ID                                             AS                                             PFT_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, FNCL_TRAN_TYP_ID                                   AS                                   FNCL_TRAN_TYP_ID 
		, PFT_AMT                                            AS                                            PFT_AMT 
		, cast( PFT_DT as DATE )                             AS                                             PFT_DT 
		, cast( PFT_DUE_DT as DATE )                         AS                                         PFT_DUE_DT 
		, PFT_DRV_BAL_AMT                                    AS                                    PFT_DRV_BAL_AMT 
		, PFT_ID_RVRS                                        AS                                        PFT_ID_RVRS 
		, upper( PFT_RVRS_IND )                              AS                                       PFT_RVRS_IND 
		, cast( PFT_RVS_DUE_DT as DATE )                     AS                                     PFT_RVS_DUE_DT 
		, upper( TRIM( PFT_RVS_RSN_TXT ) )                   AS                                    PFT_RVS_RSN_TXT 
		, upper( PFT_RVS_REQD_IND )                          AS                                   PFT_RVS_REQD_IND 
		, cast( PFT_EFT_PYMT_PRCSR_SENT_DT as DATE )         AS                         PFT_EFT_PYMT_PRCSR_SENT_DT 
		, cast( PFT_SENT_DT as DATE )                        AS                                        PFT_SENT_DT 
		, upper( PFT_GNRL_LDGR_SENT_IND )                    AS                             PFT_GNRL_LDGR_SENT_IND 
		, upper( PFT_ACCT_PYBL_SENT_IND )                    AS                             PFT_ACCT_PYBL_SENT_IND 
		, upper( TRIM( PAY_TRK_SRC_TYP_CD ) )                AS                                 PAY_TRK_SRC_TYP_CD 
		, upper( TRIM( PFT_DRV_PAY_TRK_SRC_TYP_NM ) )        AS                         PFT_DRV_PAY_TRK_SRC_TYP_NM 
		, CUST_ID                                            AS                                            CUST_ID 
		, PTCP_ID                                            AS                                            PTCP_ID 
		, upper( TRIM( AGRE_TYP_CD ) )                       AS                                        AGRE_TYP_CD 
		, PREM_PRD_ID                                        AS                                        PREM_PRD_ID 
		, INVC_ID                                            AS                                            INVC_ID 
		, CLTRL_DRW_ID                                       AS                                       CLTRL_DRW_ID 
		, PYRL_RPT_ID                                        AS                                        PYRL_RPT_ID 
		, upper( TRIM( PFT_CMT ) )                           AS                                            PFT_CMT 
		, upper( PFT_EFT_IND )                               AS                                        PFT_EFT_IND 
		, upper( TRIM( JUR_TYP_CD ) )                        AS                                         JUR_TYP_CD 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		from SRC_PFT
            ),
LOGIC_FTT as ( SELECT 
		  upper( TRIM( FNCL_TRAN_TYP_CD ) )                  AS                                   FNCL_TRAN_TYP_CD 
		, upper( TRIM( FNCL_TRAN_TYP_NM ) )                  AS                                   FNCL_TRAN_TYP_NM 
		, FNCL_TRAN_TYP_ID                                   AS                                   FNCL_TRAN_TYP_ID 
		from SRC_FTT
            ),
LOGIC_PTST as ( SELECT 
		  upper( TRIM( PAY_TRK_SRC_TYP_NM ) )                AS                                 PAY_TRK_SRC_TYP_NM 
		, upper( TRIM( PAY_TRK_SRC_TYP_CD ) )                AS                                 PAY_TRK_SRC_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PTST
            ),
LOGIC_BPFT as ( SELECT 
		  upper( TRIM( CLM_NO ) )                            AS                                             CLM_NO 
		, upper( TRIM( FNCL_TRAN_SUB_TYP_CD ) )              AS                               FNCL_TRAN_SUB_TYP_CD 
		, PLCY_PRD_AUDT_DTL_ID                               AS                               PLCY_PRD_AUDT_DTL_ID 
		, RPT_YR                                             AS                                             RPT_YR 
		, cast( PFT_FNCL_AUTH_SENT_DT as DATE )              AS                              PFT_FNCL_AUTH_SENT_DT 
		, PFT_ID                                             AS                                             PFT_ID 
		from SRC_BPFT
            ),
LOGIC_BFTST as ( SELECT 
		  upper( TRIM( FNCL_TRAN_SUB_TYP_NM ) )              AS                               FNCL_TRAN_SUB_TYP_NM 
		, upper( TRIM( FNCL_TRAN_SUB_TYP_CD ) )              AS                               FNCL_TRAN_SUB_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BFTST
            ),
LOGIC_PP as ( SELECT 
		  PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, TRIM( PLCY_NO )                                    AS                                            PLCY_NO 
		from SRC_PP
            ),
LOGIC_PP2 as ( SELECT DISTINCT
		  AGRE_ID                                            AS                                            AGRE_ID 
		, TRIM( PLCY_NO )                                    AS                                            PLCY_NO 
		from SRC_PP2
            )

---- RENAME LAYER ----
,

RENAME_PFT as ( SELECT 
		  PFT_ID                                             as                                             PFT_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID
		, PFT_AMT                                            as                                            PFT_AMT
		, PFT_DT                                             as                                             PFT_DT
		, PFT_DUE_DT                                         as                                         PFT_DUE_DT
		, PFT_DRV_BAL_AMT                                    as                                    PFT_DRV_BAL_AMT
		, PFT_ID_RVRS                                        as                                        PFT_ID_RVRS
		, PFT_RVRS_IND                                       as                                       PFT_RVRS_IND
		, PFT_RVS_DUE_DT                                     as                                     PFT_RVS_DUE_DT
		, PFT_RVS_RSN_TXT                                    as                                    PFT_RVS_RSN_TXT
		, PFT_RVS_REQD_IND                                   as                                   PFT_RVS_REQD_IND
		, PFT_EFT_PYMT_PRCSR_SENT_DT                         as                         PFT_EFT_PYMT_PRCSR_SENT_DT
		, PFT_SENT_DT                                        as                                        PFT_SENT_DT
		, PFT_GNRL_LDGR_SENT_IND                             as                             PFT_GNRL_LDGR_SENT_IND
		, PFT_ACCT_PYBL_SENT_IND                             as                             PFT_ACCT_PYBL_SENT_IND
		, PAY_TRK_SRC_TYP_CD                                 as                                 PAY_TRK_SRC_TYP_CD
		, PFT_DRV_PAY_TRK_SRC_TYP_NM                         as                         PFT_DRV_PAY_TRK_SRC_TYP_NM
		, CUST_ID                                            as                                            CUST_ID
		, PTCP_ID                                            as                                            PTCP_ID
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
		, PREM_PRD_ID                                        as                                        PREM_PRD_ID
		, INVC_ID                                            as                                            INVC_ID
		, CLTRL_DRW_ID                                       as                                       CLTRL_DRW_ID
		, PYRL_RPT_ID                                        as                                        PYRL_RPT_ID
		, PFT_CMT                                            as                                            PFT_CMT
		, PFT_EFT_IND                                        as                                        PFT_EFT_IND
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_PFT   ), 
RENAME_FTT as ( SELECT 
		  FNCL_TRAN_TYP_CD                                   as                                   FNCL_TRAN_TYP_CD
		, FNCL_TRAN_TYP_NM                                   as                                   FNCL_TRAN_TYP_NM
		, FNCL_TRAN_TYP_ID                                   as                               FTT_FNCL_TRAN_TYP_ID 
				FROM     LOGIC_FTT   ), 
RENAME_PTST as ( SELECT 
		  PAY_TRK_SRC_TYP_NM                                 as                                 PAY_TRK_SRC_TYP_NM
		, PAY_TRK_SRC_TYP_CD                                 as                            PTST_PAY_TRK_SRC_TYP_CD
		, VOID_IND                                           as                                      PTST_VOID_IND 
				FROM     LOGIC_PTST   ), 
RENAME_BPFT as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, FNCL_TRAN_SUB_TYP_CD                               as                               FNCL_TRAN_SUB_TYP_CD
		, PLCY_PRD_AUDT_DTL_ID                               as                               PLCY_PRD_AUDT_DTL_ID
		, RPT_YR                                             as                                             RPT_YR
		, PFT_FNCL_AUTH_SENT_DT                              as                              PFT_FNCL_AUTH_SENT_DT
		, PFT_ID                                             as                                        BPFT_PFT_ID 
				FROM     LOGIC_BPFT   ), 
RENAME_BFTST as ( SELECT 
		  FNCL_TRAN_SUB_TYP_NM                               as                               FNCL_TRAN_SUB_TYP_NM
		, FNCL_TRAN_SUB_TYP_CD                               as                         BFTST_FNCL_TRAN_SUB_TYP_CD
		, VOID_IND                                           as                                     BFTST_VOID_IND 
				FROM     LOGIC_BFTST   ), 
RENAME_PP as ( SELECT 
		  PLCY_PRD_ID                                        as                                     PP_PLCY_PRD_ID
		, PLCY_NO                                            as                                         PP_PLCY_NO 
				FROM     LOGIC_PP   ), 
RENAME_PP2 as ( SELECT 
		  AGRE_ID                                            as                                        PP2_AGRE_ID
		, PLCY_NO                                            as                                        PP2_PLCY_NO 
				FROM     LOGIC_PP2   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PFT                            as ( SELECT * from    RENAME_PFT   ),
FILTER_BPFT                           as ( SELECT * from    RENAME_BPFT   ),
FILTER_FTT                            as ( SELECT * from    RENAME_FTT   ),
FILTER_PTST                           as ( SELECT * from    RENAME_PTST 
				WHERE PTST_VOID_IND = 'N'  ),
FILTER_PP                             as ( SELECT * from    RENAME_PP   ),
FILTER_PP2                            as ( SELECT * from    RENAME_PP2   ),
FILTER_BFTST                          as ( SELECT * from    RENAME_BFTST 
				WHERE BFTST_VOID_IND = 'N'  ),

---- JOIN LAYER ----

BPFT as ( SELECT * 
				FROM  FILTER_BPFT
				LEFT JOIN FILTER_BFTST ON  FILTER_BPFT.FNCL_TRAN_SUB_TYP_CD =  FILTER_BFTST.BFTST_FNCL_TRAN_SUB_TYP_CD  ),
PFT as ( SELECT * , 
        COALESCE(FILTER_PP.PP_PLCY_NO, FILTER_PP2.PP2_PLCY_NO) AS PLCY_NO
				FROM  FILTER_PFT
				LEFT JOIN BPFT ON  FILTER_PFT.PFT_ID = BPFT.BPFT_PFT_ID 
						LEFT JOIN FILTER_FTT ON  FILTER_PFT.FNCL_TRAN_TYP_ID =  FILTER_FTT.FTT_FNCL_TRAN_TYP_ID 
								LEFT JOIN FILTER_PTST ON  FILTER_PFT.PAY_TRK_SRC_TYP_CD =  FILTER_PTST.PTST_PAY_TRK_SRC_TYP_CD 
								LEFT JOIN FILTER_PP ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_PP.PP_PLCY_PRD_ID 
								LEFT JOIN FILTER_PP2 ON  FILTER_PFT.AGRE_ID =  FILTER_PP2.PP2_AGRE_ID  )
SELECT * 
from PFT