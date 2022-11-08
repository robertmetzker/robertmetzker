---- SRC LAYER ----
WITH
SRC_PSRH as ( SELECT *     from     STAGING.STG_POLICY_STATUS_REASON_HISTORY ),
SRC_PLCY as ( SELECT *     from     STAGING.STG_POLICY ),
//SRC_PSRH as ( SELECT *     from     STG_POLICY_STATUS_REASON_HISTORY) ,
//SRC_PLCY as ( SELECT *     from     STG_POLICY) ,

---- LOGIC LAYER ----

LOGIC_PSRH as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, TRIM( PLCY_TYP_CODE )                              as                                      PLCY_TYP_CODE 
		, TRIM( PLCY_TYP_NAME )                              as                                      PLCY_TYP_NAME 
		, AGRE_ID                                            as                                            AGRE_ID 
		, CASE WHEN PLCY_TYP_CODE IN ('PA', 'MIF', 'BL') AND PLCY_STS_RSN_TYP_CD IN ('RN', 'LPS_NONPAY', 'LPS_NONRPTNOPAY') AND RIGHT(PLCY_STS_RSN_CHG_EFF_DT,5) >= '07-01' THEN PLCY_PRD_EFF_DATE
               WHEN PLCY_TYP_CODE IN ('PEC','PES') AND PLCY_STS_RSN_TYP_CD IN ('RN', 'LPS_NONPAY', 'LPS_NONRPTNOPAY') AND RIGHT(PLCY_STS_RSN_CHG_EFF_DT,5) >= '01-01' THEN PLCY_PRD_EFF_DATE
                    ELSE PLCY_STS_RSN_CHG_EFF_DT  END        AS                                      STATUS_EFF_DT                                                                                                                                                   
		, PLCY_PRD_EFF_DATE                                  as                                  PLCY_PRD_EFF_DATE 
		, PLCY_PRD_END_DATE                                  as                                  PLCY_PRD_END_DATE 
		, PLCY_STS_TRANS_DT                                  as                                  PLCY_STS_TRANS_DT 
		, PLCY_STS_RSN_TRANS_DT                              as                              PLCY_STS_RSN_TRANS_DT 
		, PLCY_STS_RSN_CHG_EFF_DT                            as                            PLCY_STS_RSN_CHG_EFF_DT 
		, PLCY_STS_RSN_TRAN_DUE_DT                           as                           PLCY_STS_RSN_TRAN_DUE_DT 
		, TRIM( PAYOFF_OPT_TYP_CD )                          as                                  PAYOFF_OPT_TYP_CD 
		, TRIM( PLCY_STS_TYP_CD )                            as                                    PLCY_STS_TYP_CD 
		, TRIM( PLCY_STS_TYP_NM )                            as                                    PLCY_STS_TYP_NM 
		, TRIM( PLCY_STS_RSN_TYP_CD )                        as                                PLCY_STS_RSN_TYP_CD 
		, TRIM( PLCY_STS_RSN_TYP_NM )                        as                                PLCY_STS_RSN_TYP_NM 
		, TRIM( CANC_METH_TYP_CD )                           as                                   CANC_METH_TYP_CD 
		, TRIM( CANC_METH_TYP_NM )                           as                                   CANC_METH_TYP_NM 
		, PLCY_STS_CANC_DATE                                 as                                 PLCY_STS_CANC_DATE 
		, TRIM( CANC_CMT )                                   as                                           CANC_CMT 
		, PSH_HIST_EFF_DTM                                   as                                   PSH_HIST_EFF_DTM 
		, PSH_HIST_END_DTM                                   as                                   PSH_HIST_END_DTM 
		, PSR_HIST_EFF_DTM                                   as                                   PSR_HIST_EFF_DTM 
		, PSR_HIST_END_DTM                                   as                                   PSR_HIST_END_DTM 
		, TRIM( CRNT_PLCY_PRD_STS_RSN_IND )                  as                          CRNT_PLCY_PRD_STS_RSN_IND 
		, TRIM( MOST_RCNT_PLCY_STS_RSN_IND )                 as                         MOST_RCNT_PLCY_STS_RSN_IND 
		, AUTH_ID                                            as                                            AUTH_ID 
		, PFT_ID                                             as                                             PFT_ID 
		, PYRL_RPT_ID                                        as                                        PYRL_RPT_ID 
		, PSH_AUDIT_USER_ID_CREA                             as                             PSH_AUDIT_USER_ID_CREA 
		, PSH_AUDIT_USER_CREA_DTM                            as                            PSH_AUDIT_USER_CREA_DTM 
		, PSH_AUDIT_USER_ID_UPDT                             as                             PSH_AUDIT_USER_ID_UPDT 
		, PSH_AUDIT_USER_UPDT_DTM                            as                            PSH_AUDIT_USER_UPDT_DTM 
		, PSR_AUDIT_USER_ID_CREA                             as                             PSR_AUDIT_USER_ID_CREA 
		, PSR_AUDIT_USER_CREA_DTM                            as                            PSR_AUDIT_USER_CREA_DTM 
		, PSR_AUDIT_USER_ID_UPDT                             as                             PSR_AUDIT_USER_ID_UPDT 
		, PSR_AUDIT_USER_UPDT_DTM                            as                            PSR_AUDIT_USER_UPDT_DTM 
		, TRIM( PAYOFF_OPT_TYP_NM )                          as                                  PAYOFF_OPT_TYP_NM 
		, TRIM( PAYOFF_OPT_TYP_DFLT_IND )                    as                            PAYOFF_OPT_TYP_DFLT_IND 
		from SRC_PSRH
            ),
LOGIC_PLCY as ( SELECT 
		  TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( AGRE_TYP_CD )                                as                                        AGRE_TYP_CD 
		, TRIM( AGRE_TYP_NM )                                as                                        AGRE_TYP_NM 
		, CUST_ID_ACCT_HLDR                                  as                                  CUST_ID_ACCT_HLDR 
		, TRIM( CUST_NO )                                    as                                            CUST_NO 
		, PLCY_ORIG_DT                                       as                                       PLCY_ORIG_DT 
		, TRIM( STRAIGHT_THRU_PROCESSING_IND )               as                       STRAIGHT_THRU_PROCESSING_IND 
		, TRIM( PLCY_SHLL_IND )                              as                                      PLCY_SHLL_IND 
		, TRIM( PLCY_KY_ACCT_IND )                           as                                   PLCY_KY_ACCT_IND 
		, TRIM( PLCY_OUT_OF_CYC_IND )                        as                                PLCY_OUT_OF_CYC_IND 
		from SRC_PLCY
            )

---- RENAME LAYER ----
,

RENAME_PSRH as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PLCY_NO                                            as                                            PLCY_NO
		, PLCY_TYP_CODE                                      as                                      PLCY_TYP_CODE
		, PLCY_TYP_NAME                                      as                                      PLCY_TYP_NAME
		, AGRE_ID                                            as                                            AGRE_ID
		, STATUS_EFF_DT                                      as                                      STATUS_EFF_DT
		, PLCY_PRD_EFF_DATE                                  as                                  PLCY_PRD_EFF_DATE
		, PLCY_PRD_END_DATE                                  as                                  PLCY_PRD_END_DATE
		, PLCY_STS_TRANS_DT                                  as                                  PLCY_STS_TRANS_DT
		, PLCY_STS_RSN_TRANS_DT                              as                              PLCY_STS_RSN_TRANS_DT
		, PLCY_STS_RSN_CHG_EFF_DT                            as                            PLCY_STS_RSN_CHG_EFF_DT
		, PLCY_STS_RSN_TRAN_DUE_DT                           as                           PLCY_STS_RSN_TRAN_DUE_DT
		, PAYOFF_OPT_TYP_CD                                  as                                  PAYOFF_OPT_TYP_CD
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_TYP_NM                                    as                                    PLCY_STS_TYP_NM
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, PLCY_STS_RSN_TYP_NM                                as                                PLCY_STS_RSN_TYP_NM
		, CANC_METH_TYP_CD                                   as                                   CANC_METH_TYP_CD
		, CANC_METH_TYP_NM                                   as                                   CANC_METH_TYP_NM
		, PLCY_STS_CANC_DATE                                 as                                 PLCY_STS_CANC_DATE
		, CANC_CMT                                           as                                           CANC_CMT
		, PSH_HIST_EFF_DTM                                   as                                   PSH_HIST_EFF_DTM
		, PSH_HIST_END_DTM                                   as                                   PSH_HIST_END_DTM
		, PSR_HIST_EFF_DTM                                   as                                   PSR_HIST_EFF_DTM
		, PSR_HIST_END_DTM                                   as                                   PSR_HIST_END_DTM
		, CRNT_PLCY_PRD_STS_RSN_IND                          as                          CRNT_PLCY_PRD_STS_RSN_IND
		, MOST_RCNT_PLCY_STS_RSN_IND                         as                         MOST_RCNT_PLCY_STS_RSN_IND
		, AUTH_ID                                            as                                            AUTH_ID
		, PFT_ID                                             as                                             PFT_ID
		, PYRL_RPT_ID                                        as                                        PYRL_RPT_ID
		, PSH_AUDIT_USER_ID_CREA                             as                             PSH_AUDIT_USER_ID_CREA
		, PSH_AUDIT_USER_CREA_DTM                            as                            PSH_AUDIT_USER_CREA_DTM
		, PSH_AUDIT_USER_ID_UPDT                             as                             PSH_AUDIT_USER_ID_UPDT
		, PSH_AUDIT_USER_UPDT_DTM                            as                            PSH_AUDIT_USER_UPDT_DTM
		, PSR_AUDIT_USER_ID_CREA                             as                             PSR_AUDIT_USER_ID_CREA
		, PSR_AUDIT_USER_CREA_DTM                            as                            PSR_AUDIT_USER_CREA_DTM
		, PSR_AUDIT_USER_ID_UPDT                             as                             PSR_AUDIT_USER_ID_UPDT
		, PSR_AUDIT_USER_UPDT_DTM                            as                            PSR_AUDIT_USER_UPDT_DTM
		, PAYOFF_OPT_TYP_NM                                  as                                  PAYOFF_OPT_TYP_NM
		, PAYOFF_OPT_TYP_DFLT_IND                            as                            PAYOFF_OPT_TYP_DFLT_IND 
				FROM     LOGIC_PSRH   ), 
RENAME_PLCY as ( SELECT 
		  PLCY_NO                                            as                                       PLCY_PLCY_NO
		, AGRE_ID                                            as                                       PLCY_AGRE_ID
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
		, AGRE_TYP_NM                                        as                                        AGRE_TYP_NM
		, CUST_ID_ACCT_HLDR                                  as                                  CUST_ID_ACCT_HLDR
		, CUST_NO                                            as                                            CUST_NO
		, PLCY_ORIG_DT                                       as                                       PLCY_ORIG_DT
		, STRAIGHT_THRU_PROCESSING_IND                       as                       STRAIGHT_THRU_PROCESSING_IND
		, PLCY_SHLL_IND                                      as                                      PLCY_SHLL_IND
		, PLCY_KY_ACCT_IND                                   as                                   PLCY_KY_ACCT_IND
		, PLCY_OUT_OF_CYC_IND                                as                                PLCY_OUT_OF_CYC_IND 
				FROM     LOGIC_PLCY   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PSRH as ( SELECT *, LEAD(STATUS_EFF_DT) OVER (PARTITION BY PLCY_NO ORDER BY  STATUS_EFF_DT,PLCY_PRD_EFF_DATE ,PLCY_STS_TRANS_DT)-1 AS STATUS_END_DT 
		from    RENAME_PSRH 
                                            WHERE PLCY_TYP_CODE <> 'SI' AND PLCY_STS_TYP_CD <> 'EXP' 
QUALIFY STATUS_EFF_DT <= COALESCE(STATUS_END_DT,'2099-12-31') OR PLCY_STS_RSN_TYP_CD IN ('PLCY_ISS','ISS_PLCY','LPS_NONPAY_INST','RN') 
               ORDER BY PLCY_PRD_EFF_DATE, PSH_HIST_EFF_DTM, PSR_HIST_EFF_DTM
               ),
FILTER_PLCY                           as ( SELECT * from    RENAME_PLCY   ),

---- JOIN LAYER ----

PSRH as ( SELECT * 
				FROM  FILTER_PSRH
				LEFT JOIN FILTER_PLCY ON  FILTER_PSRH.PLCY_NO =  FILTER_PLCY.PLCY_PLCY_NO  )
SELECT 
	PLCY_PRD_ID
,	PLCY_NO
,	PLCY_TYP_CODE
,	PLCY_TYP_NAME
,	AGRE_ID
,	STATUS_EFF_DT
,   STATUS_END_DT  
,	PLCY_PRD_EFF_DATE
,	PLCY_PRD_END_DATE
,	PLCY_STS_TRANS_DT
,	PLCY_STS_RSN_TRANS_DT
,	PLCY_STS_RSN_CHG_EFF_DT
,	PLCY_STS_RSN_TRAN_DUE_DT
,	PAYOFF_OPT_TYP_CD
,	PLCY_STS_TYP_CD
,	PLCY_STS_TYP_NM
,	PLCY_STS_RSN_TYP_CD
,	PLCY_STS_RSN_TYP_NM
,	CANC_METH_TYP_CD
,	CANC_METH_TYP_NM
,	PLCY_STS_CANC_DATE
,	CANC_CMT
,	PSH_HIST_EFF_DTM
,	PSH_HIST_END_DTM
,	PSR_HIST_EFF_DTM
,	PSR_HIST_END_DTM
,	CRNT_PLCY_PRD_STS_RSN_IND
,	MOST_RCNT_PLCY_STS_RSN_IND
,	AUTH_ID
,	PFT_ID
,	PYRL_RPT_ID
,	PSH_AUDIT_USER_ID_CREA
,	PSH_AUDIT_USER_CREA_DTM
,	PSH_AUDIT_USER_ID_UPDT
,	PSH_AUDIT_USER_UPDT_DTM
,	PSR_AUDIT_USER_ID_CREA
,	PSR_AUDIT_USER_CREA_DTM
,	PSR_AUDIT_USER_ID_UPDT
,	PSR_AUDIT_USER_UPDT_DTM
,	PAYOFF_OPT_TYP_NM
,	PAYOFF_OPT_TYP_DFLT_IND
,	to_varchar(psh_hist_eff_dtm, 'HH12:MI:SS') as TIME
,	CASE WHEN PLCY_STS_TYP_CD IN ('ACT') THEN 'Y' ELSE 'N'end as POLICY_ACTIVE_IND
,	CAST((PLCY_PRD_EFF_DATE || ' - ' || PLCY_PRD_END_DATE) as TEXT)  as   POLICY_PERIOD_DESC
,	CASE WHEN PLCY_TYP_CODE IN ('PEC') THEN 'Y' ELSE 'N'end as PEC_POLICY_IND
,	CASE WHEN STATUS_EFF_DT = min(PLCY_PRD_EFF_DATE) over (partition by PLCY_NO) THEN 'Y' ELSE 'N' END AS  NEW_POLICY_IND
,	PLCY_PLCY_NO
,	PLCY_AGRE_ID
,	AGRE_TYP_CD
,	AGRE_TYP_NM
,	CUST_ID_ACCT_HLDR
,	CUST_NO
,	PLCY_ORIG_DT
,	STRAIGHT_THRU_PROCESSING_IND
,	PLCY_SHLL_IND
,	PLCY_KY_ACCT_IND
,	PLCY_OUT_OF_CYC_IND
,	CASE WHEN PLCY_STS_TYP_CD IN ('ACT') AND PLCY_STS_RSN_TYP_CD IN ('RN') AND LAG(PLCY_STS_RSN_TYP_CD) OVER (PARTITION BY PLCY_NO ORDER BY PLCY_PRD_EFF_DATE, PSH_HIST_EFF_DTM, PSR_HIST_EFF_DTM)
       NOT IN ('LPS_NONPAY_INST', 'LPS_NONPAY','LPS_NONRPTNOPAY')
       AND STATUS_EFF_DT < COALESCE(STATUS_END_DT,current_date())  THEN (COALESCE(STATUS_END_DT,current_date())-STATUS_EFF_DT)
    WHEN PLCY_STS_TYP_CD IN ('ACT') AND PLCY_STS_RSN_TYP_CD IN ('PLCY_ISS','DBTR_IN_POSS','RNST_FPAY_RECV', 'RNST_EXP','ISS_PLCY', 'RNST_MINPAYRCVD','RECEIVERSHIP','RNST_PAYPLNPLC','RNST_FULPAYRCVD') AND STATUS_EFF_DT < COALESCE(STATUS_END_DT,current_date()) 
         THEN (COALESCE(STATUS_END_DT,current_date())-STATUS_EFF_DT)
                ELSE NULL END AS COVERED_DAYS
,	CASE WHEN PLCY_STS_TYP_CD IN ('ACT', 'CAN') AND PLCY_STS_RSN_TYP_CD IN ('BNKRPT_CANC', 'AGY_REQ_NP','NO_COV','LPS_NONPAY_INST','BNKRPT_CANC', 'LPS_NONPAY','UNCLCTBL','CANC_REWRT', 
               'LPS_NONRPTNOPAY', 'INS_REQ_BS','BNKRPT_COMB','INS_REQ_WC','INS_REQ_NOPLCY','INS_REQ_OB','BWC_INITATD','LPS_NONPAY_INST') AND STATUS_EFF_DT < COALESCE(STATUS_END_DT,current_date()) 
          THEN (COALESCE(STATUS_END_DT,current_date())-STATUS_EFF_DT) ELSE NULL END AS NON_COVERED_DAYS
,	CASE WHEN PLCY_STS_TYP_CD IN ('ACT') AND PLCY_STS_RSN_TYP_CD IN ('LPS_NONPAY_INST', 'LPS_NONPAY','LPS_NONRPTNOPAY') AND STATUS_EFF_DT < COALESCE(STATUS_END_DT,current_date()) 
         THEN (COALESCE(STATUS_END_DT,current_date())-STATUS_EFF_DT) ELSE NULL  END AS LAPSED_DAYS

from PSRH