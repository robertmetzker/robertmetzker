---- SRC LAYER ----
WITH
SRC_CFT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_FINANCIAL_TRANSACTION ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
SRC_FTT as ( SELECT *     from     DEV_VIEWS.PCMP.FINANCIAL_TRANSACTION_TYPE ),
SRC_BT as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_TYPE ),
SRC_BCYC as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_CYCLE_TYPE ),
SRC_BCTG as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_CATEGORY_TYPE ),
SRC_PT as ( SELECT *     from     DEV_VIEWS.PCMP.PAYMENT_TYPE ),
SRC_BRT as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_REPORTING_TYPE ),
//SRC_CFT as ( SELECT *     from     CLAIM_FINANCIAL_TRANSACTION) ,
//SRC_C as ( SELECT *     from     CLAIM) ,
//SRC_FTT as ( SELECT *     from     FINANCIAL_TRANSACTION_TYPE) ,
//SRC_BT as ( SELECT *     from     BENEFIT_TYPE) ,
//SRC_BCYC as ( SELECT *     from     BENEFIT_CYCLE_TYPE) ,
//SRC_BCTG as ( SELECT *     from     BENEFIT_CATEGORY_TYPE) ,
//SRC_PT as ( SELECT *     from     PAYMENT_TYPE) ,
//SRC_BRT as ( SELECT *     from     BENEFIT_REPORTING_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CFT as ( SELECT 
		  CFT_ID                                             AS                                             CFT_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, FNCL_TRAN_TYP_ID                                   AS                                   FNCL_TRAN_TYP_ID 
		, cast( CFT_DT as DATE )                             AS                                             CFT_DT 
		, cast( CFT_DUE_DT as DATE )                         AS                                         CFT_DUE_DT 
		, CFT_AMT                                            AS                                            CFT_AMT 
		, CFT_DRV_BAL_AMT                                    AS                                    CFT_DRV_BAL_AMT 
		, CFT_ID_RVRS_ID                                     AS                                     CFT_ID_RVRS_ID 
		, upper( CFT_RVRS_IND )                              AS                                       CFT_RVRS_IND 
		, cast( CFT_SENT_DT as DATE )                        AS                                        CFT_SENT_DT 
		, CUST_ID                                            AS                                            CUST_ID 
		, PTCP_ID                                            AS                                            PTCP_ID 
		, upper( TRIM( BNFT_TYP_CD ) )                       AS                                        BNFT_TYP_CD 
		, upper( TRIM( PAY_TYP_CD ) )                        AS                                         PAY_TYP_CD 
		, BNFT_RPT_TYP_ID                                    AS                                    BNFT_RPT_TYP_ID 
		, upper( TRIM( CFT_CMT ) )                           AS                                            CFT_CMT 
		, PAY_BTCH_ID                                        AS                                        PAY_BTCH_ID 
		, PAY_BTCH_DTL_ID                                    AS                                    PAY_BTCH_DTL_ID 
		, upper( CFT_NOTE_IND )                              AS                                       CFT_NOTE_IND 
		, upper( TRIM( CST_CNTR_TYP_CD ) )                   AS                                    CST_CNTR_TYP_CD 
		, upper( CFT_CNSM_IND )                              AS                                       CFT_CNSM_IND 
		, upper( CFT_LNK_IND )                               AS                                        CFT_LNK_IND 
		, upper( CFT_INDM_IND )                              AS                                       CFT_INDM_IND 
		, upper( CFT_MED_IND )                               AS                                        CFT_MED_IND 
		, upper( CFT_EXP_IND )                               AS                                        CFT_EXP_IND 
		, upper( CFT_GNRL_LDGR_SENT_IND )                    AS                             CFT_GNRL_LDGR_SENT_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		from SRC_CFT
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CLM_NO ) )                            AS                                             CLM_NO 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( CLM_REL_SNPSHT_IND )                        AS                                 CLM_REL_SNPSHT_IND 
		from SRC_C
            ),
LOGIC_FTT as ( SELECT 
		  upper( TRIM( FNCL_TRAN_TYP_CD ) )                  AS                                   FNCL_TRAN_TYP_CD 
		, upper( TRIM( FNCL_TRAN_TYP_NM ) )                  AS                                   FNCL_TRAN_TYP_NM 
		, upper( FNCL_TRAN_TYP_GNRL_LDGR_IND )               AS                        FNCL_TRAN_TYP_GNRL_LDGR_IND 
		, upper( FNCL_TRAN_TYP_ACCT_PYBL_IND )               AS                        FNCL_TRAN_TYP_ACCT_PYBL_IND 
		, FNCL_TRAN_TYP_ID                                   AS                                   FNCL_TRAN_TYP_ID 
		from SRC_FTT
            ),
LOGIC_BT as ( SELECT 
		  upper( TRIM( BNFT_TYP_NM ) )                       AS                                        BNFT_TYP_NM 
		, upper( TRIM( BNFT_CYC_TYP_CD ) )                   AS                                    BNFT_CYC_TYP_CD 
		, upper( TRIM( BNFT_CTG_TYP_CD ) )                   AS                                    BNFT_CTG_TYP_CD 
		, cast( BNFT_TYP_EFF_DT as DATE )                    AS                                    BNFT_TYP_EFF_DT 
		, cast( BNFT_TYP_END_DT as DATE )                    AS                                    BNFT_TYP_END_DT 
		, upper( TRIM( BNFT_TYP_CD ) )                       AS                                        BNFT_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BT
            ),
LOGIC_BCYC as ( SELECT 
		  upper( TRIM( BNFT_CYC_TYP_NM ) )                   AS                                    BNFT_CYC_TYP_NM 
		, upper( TRIM( BNFT_CYC_TYP_CD ) )                   AS                                    BNFT_CYC_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BCYC
            ),
LOGIC_BCTG as ( SELECT 
		  upper( TRIM( BNFT_CTG_TYP_NM ) )                   AS                                    BNFT_CTG_TYP_NM 
		, upper( TRIM( BNFT_CTG_TYP_CD ) )                   AS                                    BNFT_CTG_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BCTG
            ),
LOGIC_PT as ( SELECT 
		  upper( TRIM( PAY_TYP_NM ) )                        AS                                         PAY_TYP_NM 
		, upper( PAY_TYP_BILL_VSBL_IND )                     AS                              PAY_TYP_BILL_VSBL_IND 
		, upper( TRIM( PAY_TYP_CD ) )                        AS                                         PAY_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PT
            ),
LOGIC_BRT as ( SELECT 
		  upper( TRIM( BNFT_RPT_TYP_NM ) )                   AS                                    BNFT_RPT_TYP_NM 
		, BNFT_RPT_TYP_ID                                    AS                                    BNFT_RPT_TYP_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BRT
            )

---- RENAME LAYER ----
,

RENAME_CFT as ( SELECT 
		  CFT_ID                                             as                                             CFT_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID
		, CFT_DT                                             as                                             CFT_DT
		, CFT_DUE_DT                                         as                                         CFT_DUE_DT
		, CFT_AMT                                            as                                            CFT_AMT
		, CFT_DRV_BAL_AMT                                    as                                    CFT_DRV_BAL_AMT
		, CFT_ID_RVRS_ID                                     as                                     CFT_ID_RVRS_ID
		, CFT_RVRS_IND                                       as                                       CFT_RVRS_IND
		, CFT_SENT_DT                                        as                                        CFT_SENT_DT
		, CUST_ID                                            as                                            CUST_ID
		, PTCP_ID                                            as                                            PTCP_ID
		, BNFT_TYP_CD                                        as                                        BNFT_TYP_CD
		, PAY_TYP_CD                                         as                                         PAY_TYP_CD
		, BNFT_RPT_TYP_ID                                    as                                    BNFT_RPT_TYP_ID
		, CFT_CMT                                            as                                            CFT_CMT
		, PAY_BTCH_ID                                        as                                        PAY_BTCH_ID
		, PAY_BTCH_DTL_ID                                    as                                    PAY_BTCH_DTL_ID
		, CFT_NOTE_IND                                       as                                       CFT_NOTE_IND
		, CST_CNTR_TYP_CD                                    as                                    CST_CNTR_TYP_CD
		, CFT_CNSM_IND                                       as                                       CFT_CNSM_IND
		, CFT_LNK_IND                                        as                                        CFT_LNK_IND
		, CFT_INDM_IND                                       as                                       CFT_INDM_IND
		, CFT_MED_IND                                        as                                        CFT_MED_IND
		, CFT_EXP_IND                                        as                                        CFT_EXP_IND
		, CFT_GNRL_LDGR_SENT_IND                             as                             CFT_GNRL_LDGR_SENT_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_CFT   ), 
RENAME_C as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                          C_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
				FROM     LOGIC_C   ), 
RENAME_FTT as ( SELECT 
		  FNCL_TRAN_TYP_CD                                   as                                   FNCL_TRAN_TYP_CD
		, FNCL_TRAN_TYP_NM                                   as                                   FNCL_TRAN_TYP_NM
		, FNCL_TRAN_TYP_GNRL_LDGR_IND                        as                        FNCL_TRAN_TYP_GNRL_LDGR_IND
		, FNCL_TRAN_TYP_ACCT_PYBL_IND                        as                        FNCL_TRAN_TYP_ACCT_PYBL_IND
		, FNCL_TRAN_TYP_ID                                   as                               FTT_FNCL_TRAN_TYP_ID 
				FROM     LOGIC_FTT   ), 
RENAME_BT as ( SELECT 
		  BNFT_TYP_NM                                        as                                        BNFT_TYP_NM
		, BNFT_CYC_TYP_CD                                    as                                    BNFT_CYC_TYP_CD
		, BNFT_CTG_TYP_CD                                    as                                    BNFT_CTG_TYP_CD
		, BNFT_TYP_EFF_DT                                    as                                    BNFT_TYP_EFF_DT
		, BNFT_TYP_END_DT                                    as                                    BNFT_TYP_END_DT
		, BNFT_TYP_CD                                        as                                     BT_BNFT_TYP_CD
		, VOID_IND                                           as                                        BT_VOID_IND 
				FROM     LOGIC_BT   ), 
RENAME_BCYC as ( SELECT 
		  BNFT_CYC_TYP_NM                                    as                                    BNFT_CYC_TYP_NM
		, BNFT_CYC_TYP_CD                                    as                               BCYC_BNFT_CYC_TYP_CD
		, VOID_IND                                           as                                      BCYC_VOID_IND 
				FROM     LOGIC_BCYC   ), 
RENAME_BCTG as ( SELECT 
		  BNFT_CTG_TYP_NM                                    as                                    BNFT_CTG_TYP_NM
		, BNFT_CTG_TYP_CD                                    as                               BCTG_BNFT_CTG_TYP_CD
		, VOID_IND                                           as                                      BCTG_VOID_IND 
				FROM     LOGIC_BCTG   ), 
RENAME_PT as ( SELECT 
		  PAY_TYP_NM                                         as                                         PAY_TYP_NM
		, PAY_TYP_BILL_VSBL_IND                              as                              PAY_TYP_BILL_VSBL_IND
		, PAY_TYP_CD                                         as                                      PT_PAY_TYP_CD
		, VOID_IND                                           as                                        PT_VOID_IND 
				FROM     LOGIC_PT   ), 
RENAME_BRT as ( SELECT 
		  BNFT_RPT_TYP_NM                                    as                                    BNFT_RPT_TYP_NM
		, BNFT_RPT_TYP_ID                                    as                                BRT_BNFT_RPT_TYP_ID
		, VOID_IND                                           as                                       BRT_VOID_IND 
				FROM     LOGIC_BRT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CFT                            as ( SELECT * from    RENAME_CFT   ),
FILTER_C                              as ( SELECT * from    RENAME_C 
				WHERE CLM_REL_SNPSHT_IND = 'N'  ),
FILTER_FTT                            as ( SELECT * from    RENAME_FTT   ),
FILTER_PT                             as ( SELECT * from    RENAME_PT 
				WHERE PT_VOID_IND = 'N'  ),
FILTER_BT                             as ( SELECT * from    RENAME_BT   ),
FILTER_BRT                            as ( SELECT * from    RENAME_BRT   ),
FILTER_BCTG                           as ( SELECT * from    RENAME_BCTG   ),
FILTER_BCYC                           as ( SELECT * from    RENAME_BCYC   ),

---- JOIN LAYER ----

BT as ( SELECT * 
				FROM  FILTER_BT
				LEFT JOIN FILTER_BCTG ON  FILTER_BT.BNFT_CTG_TYP_CD =  FILTER_BCTG.BCTG_BNFT_CTG_TYP_CD 
								LEFT JOIN FILTER_BCYC ON  FILTER_BT.BNFT_CYC_TYP_CD =  FILTER_BCYC.BCYC_BNFT_CYC_TYP_CD  ),
CFT as ( SELECT * 
				FROM  FILTER_CFT
				INNER JOIN FILTER_C ON  FILTER_CFT.AGRE_ID =  FILTER_C.C_AGRE_ID 
								LEFT JOIN FILTER_FTT ON  FILTER_CFT.FNCL_TRAN_TYP_ID =  FILTER_FTT.FTT_FNCL_TRAN_TYP_ID 
								LEFT JOIN FILTER_PT ON  FILTER_CFT.PAY_TYP_CD =  FILTER_PT.PT_PAY_TYP_CD 
						LEFT JOIN BT ON  FILTER_CFT.BNFT_TYP_CD = BT.BT_BNFT_TYP_CD 
								LEFT JOIN FILTER_BRT ON  FILTER_CFT.BNFT_RPT_TYP_ID =  FILTER_BRT.BRT_BNFT_RPT_TYP_ID  )
SELECT * 
from CFT