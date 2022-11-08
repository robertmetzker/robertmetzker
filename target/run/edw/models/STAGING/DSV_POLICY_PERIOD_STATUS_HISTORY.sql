
  create or replace  view DEV_EDW.STAGING.DSV_POLICY_PERIOD_STATUS_HISTORY  as (
    

---- SRC LAYER ----
WITH
SRC_PPSH as ( SELECT *     from     STAGING.DST_POLICY_PERIOD_STATUS_HISTORY ),
//SRC_PPSH as ( SELECT *     from     DST_POLICY_PERIOD_STATUS_HISTORY) ,

---- LOGIC LAYER ----

LOGIC_PPSH as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, PLCY_NO                                            as                                            PLCY_NO 
		, PLCY_TYP_CODE                                      as                                      PLCY_TYP_CODE 
		, PLCY_TYP_NAME                                      as                                      PLCY_TYP_NAME 
		, AGRE_ID                                            as                                            AGRE_ID 
		, STATUS_EFF_DT                                      as                                      STATUS_EFF_DT 
		, STATUS_END_DT                                      as                                      STATUS_END_DT 
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
		, TIME                                               as                                               TIME 
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC 
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND 
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND 
		, PLCY_PLCY_NO                                       as                                       PLCY_PLCY_NO 
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID 
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD 
		, AGRE_TYP_NM                                        as                                        AGRE_TYP_NM 
		, CUST_ID_ACCT_HLDR                                  as                                  CUST_ID_ACCT_HLDR 
		, CUST_NO                                            as                                            CUST_NO 
		, PLCY_ORIG_DT                                       as                                       PLCY_ORIG_DT 
		, STRAIGHT_THRU_PROCESSING_IND                       as                       STRAIGHT_THRU_PROCESSING_IND 
		, PLCY_SHLL_IND                                      as                                      PLCY_SHLL_IND 
		, PLCY_KY_ACCT_IND                                   as                                   PLCY_KY_ACCT_IND 
		, PLCY_OUT_OF_CYC_IND                                as                                PLCY_OUT_OF_CYC_IND 
		, COVERED_DAYS                                       as                                       COVERED_DAYS 
		, NON_COVERED_DAYS                                   as                                   NON_COVERED_DAYS 
		, LAPSED_DAYS                                        as                                        LAPSED_DAYS 
		from SRC_PPSH
            )

---- RENAME LAYER ----
,

RENAME_PPSH as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PLCY_NO                                            as                                            PLCY_NO
		, PLCY_TYP_CODE                                      as                                      PLCY_TYP_CODE
		, PLCY_TYP_NAME                                      as                                      PLCY_TYP_NAME
		, AGRE_ID                                            as                                            AGRE_ID
		, STATUS_EFF_DT                                      as                                      STATUS_EFF_DT
		, STATUS_END_DT                                      as                                      STATUS_END_DT
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
		, TIME                                               as                                               TIME
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND
		, PLCY_PLCY_NO                                       as                                       PLCY_PLCY_NO
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
		, AGRE_TYP_NM                                        as                                        AGRE_TYP_NM
		, CUST_ID_ACCT_HLDR                                  as                                  CUST_ID_ACCT_HLDR
		, CUST_NO                                            as                                            CUST_NO
		, PLCY_ORIG_DT                                       as                                       PLCY_ORIG_DT
		, STRAIGHT_THRU_PROCESSING_IND                       as                       STRAIGHT_THRU_PROCESSING_IND
		, PLCY_SHLL_IND                                      as                                      PLCY_SHLL_IND
		, PLCY_KY_ACCT_IND                                   as                                   PLCY_KY_ACCT_IND
		, PLCY_OUT_OF_CYC_IND                                as                                PLCY_OUT_OF_CYC_IND
		, COVERED_DAYS                                       as                                       COVERED_DAYS
		, NON_COVERED_DAYS                                   as                                   NON_COVERED_DAYS
		, LAPSED_DAYS                                        as                                        LAPSED_DAYS 
				FROM     LOGIC_PPSH   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PPSH                           as ( SELECT * from    RENAME_PPSH   ),

---- JOIN LAYER ----

 JOIN_PPSH  as  ( SELECT * 
				FROM  FILTER_PPSH )
 SELECT 
	PLCY_PRD_ID
,	PLCY_NO
,	PLCY_TYP_CODE
,	PLCY_TYP_NAME
,	AGRE_ID
,	STATUS_EFF_DT
,	STATUS_END_DT
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
,	TIME
,	POLICY_ACTIVE_IND
,	POLICY_PERIOD_DESC
,	PEC_POLICY_IND
,	NEW_POLICY_IND
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
,	COVERED_DAYS
,	NON_COVERED_DAYS
,	LAPSED_DAYS
  FROM  JOIN_PPSH
  );
