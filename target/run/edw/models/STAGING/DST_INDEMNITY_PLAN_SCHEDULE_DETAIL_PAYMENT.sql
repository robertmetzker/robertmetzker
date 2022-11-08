

      create or replace  table DEV_EDW.STAGING.DST_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT  as
      (-- Used some of the filter conditions in the logic layer
---- SRC LAYER ----
WITH
SRC_IP as ( SELECT *     from     STAGING.STG_INDEMNITY_PAYMENT ),
SRC_ISDA as ( SELECT *     from     STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT ),
SRC_CP as ( SELECT *     from     STAGING.STG_CLAIM_PARTICIPATION ),
SRC_ISS as ( SELECT *     from     STAGING.STG_INDEMNITY_SCHEDULE ),
SRC_ISD as ( SELECT *     from     STAGING.STG_INDEMNITY_SCHEDULE_DETAIL ),
SRC_U as ( SELECT *     from     STAGING.STG_USERS ),
-- SRC_AU as ( SELECT *     from     STAGING.STG_USERS ),
SRC_CFT_PAY as ( SELECT *     from     STAGING.STG_CLAIM_FINANCIAL_TRANSACTION ),
SRC_CCS as ( SELECT *     from     STAGING.STG_CUSTOMER_CHILD_SUPPORT ),
SRC_CALC as ( SELECT *     from     STAGING.STG_CALCULATION_RESULT_ATTR_GRP ),
SRC_CFTA as ( SELECT *     from     STAGING.STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED ),
SRC_HIST as ( SELECT *     from     STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_HIST ),
SRC_ISDC as ( SELECT *     from     STAGING.STG_INDM_SCH_DTL_CHLD_SUPT_XREF ),
SRC_PR as ( SELECT *     from     STAGING.STG_PAYMENT_REQUEST ),
SRC_PRS as ( SELECT *     from     STAGING.STG_PAYMENT_REQUEST_STATUS ),
SRC_XREF as ( SELECT *     from     STAGING.STG_CLAIM_FINANCIAL_TRAN_PAY_REQS ),
//SRC_IP as ( SELECT *     from     STG_INDEMNITY_PAYMENT) ,
//SRC_ISDA as ( SELECT *     from     STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT) ,
//SRC_CP as ( SELECT *     from     STG_CLAIM_PARTICIPATION) ,
//SRC_ISS as ( SELECT *     from     STG_INDEMNITY_SCHEDULE) ,
//SRC_ISD as ( SELECT *     from     STG_INDEMNITY_SCHEDULE_DETAIL) ,
//SRC_U as ( SELECT *     from     STG_USERS) ,
-- //SRC_AU as ( SELECT *     from     STG_USERS) ,
//SRC_CFT_PAY as ( SELECT *     from     STG_CLAIM_FINANCIAL_TRANSACTION) ,
//SRC_CCS as ( SELECT *     from     STG_CUSTOMER_CHILD_SUPPORT) ,
//SRC_CALC as ( SELECT *     from     STG_CALCULATION_RESULT_ATTR_GRP) ,
//SRC_CFTA as ( SELECT *     from     STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED) ,
//SRC_HIST as ( SELECT *     from     STG_INDEMNITY_SCHEDULE_DETAIL_HIST) ,
//SRC_ISDC as ( SELECT *     from     STG_INDM_SCH_DTL_CHLD_SUPT_XREF) ,
//SRC_PR as ( SELECT *     from     STG_PAYMENT_REQUEST) ,
//SRC_PRS as ( SELECT *     from     STG_PAYMENT_REQUEST_STATUS) ,
//SRC_XREF as ( SELECT *     from     STG_CLAIM_FINANCIAL_TRAN_PAY_REQS) ,

---- LOGIC LAYER ----

LOGIC_IP as ( SELECT 
		  INDM_PAY_ID                                        as                                        INDM_PAY_ID 
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( BNFT_TYP_CD )                                as                                        BNFT_TYP_CD 
		, TRIM( JUR_TYP_CD )                                 as                                         JUR_TYP_CD 
		, TRIM( BNFT_RPT_TYP_CD )                            as                                    BNFT_RPT_TYP_CD 
		, TRIM( BNFT_RPT_TYP_NM )                            as                                    BNFT_RPT_TYP_NM 
		, INDM_PAY_EFF_DT                                    as                                    INDM_PAY_EFF_DT 
		, INDM_PAY_END_DT                                    as                                    INDM_PAY_END_DT 
		, TRIM( INDM_RSN_TYP_CD )                            as                                    INDM_RSN_TYP_CD 
		, INDM_PAY_RECALC_IND                                as                                INDM_PAY_RECALC_IND 
		, VOID_IND                                           as                                           VOID_IND 
		, INDM_PAY_DRV_TOT_AMT                               as                               INDM_PAY_DRV_TOT_AMT 
		, INDM_PAY_DRV_SCH_TOT_AMT                           as                           INDM_PAY_DRV_SCH_TOT_AMT 
		, INDM_PAY_DRV_WEK                                   as                                   INDM_PAY_DRV_WEK 
		, INDM_PAY_DRV_DD                                    as                                    INDM_PAY_DRV_DD 
		, TRIM( BNFT_TYP_NM )                                as                                        BNFT_TYP_NM 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, CALC_RSLT_ID                                       as                                       CALC_RSLT_ID 
		, BNFT_CTG_TYP_CD                                    as                                    BNFT_CTG_TYP_CD
		, BNFT_CTG_TYP_NM                                    as                                    BNFT_CTG_TYP_NM
		, JUR_TYP_NM                                         as                                         JUR_TYP_NM

		from SRC_IP
            )
            ,
LOGIC_ISDA as ( SELECT 
		  INDM_SCH_DTL_AMT_ID                                as                                INDM_SCH_DTL_AMT_ID 
		, INDM_SCH_DTL_AMT_CHK_GRP_NO                        as                        INDM_SCH_DTL_AMT_CHK_GRP_NO 
		, TRIM( INDM_SCH_DTL_AMT_TYP_CD )                    as                            INDM_SCH_DTL_AMT_TYP_CD 
		, TRIM( INDM_SCH_DTL_STS_TYP_CD )                    as                            INDM_SCH_DTL_STS_TYP_CD 
		, INDM_SCH_DTL_AMT_PRI_IND                           as                           INDM_SCH_DTL_AMT_PRI_IND 
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                        INDM_SCH_DTL_AMT_MAILTO_IND 
		, INDM_SCH_DTL_AMT_RMND_IND                          as                          INDM_SCH_DTL_AMT_RMND_IND 
		, VOID_IND                                           as                                           VOID_IND 
		, INDM_SCH_DTL_AMT_AMT                               as                               INDM_SCH_DTL_AMT_AMT 
		, CFT_ID                                             as                                             CFT_ID 
		, CUST_ID                                            as                                            CUST_ID 
		, INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID 
		from SRC_ISDA
            ),
LOGIC_CP as ( SELECT 
		  TRIM( CUST_NO )                                    as                                            CUST_NO 
		, TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND 
		, CP_VOID_IND                                        as                                        CP_VOID_IND 
		, CLM_PTCP_END_DT                                    as                                    CLM_PTCP_END_DT 
		, CUST_ID                                            as                                            CUST_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		from SRC_CP
        WHERE CP_VOID_IND = 'N' AND CLM_PTCP_PRI_IND = 'Y' AND PTCP_TYP_CD IN ('ALTPAYE', 'BNFCY', 'CHLDSUPT', 'DEP', 'CLMT')
		 AND CLM_PTCP_END_DT IS NULL
            ),
LOGIC_ISS as ( SELECT 
		  TRIM( INDM_FREQ_TYP_CD )                           as                                   INDM_FREQ_TYP_CD 
		, INDM_SCH_AUTO_PAY_IND                              as                              INDM_SCH_AUTO_PAY_IND 
		, VOID_IND                                           as                                           VOID_IND 
		, INDM_SCH_FST_PRCS_DT                               as                               INDM_SCH_FST_PRCS_DT 
		, INDM_SCH_NO_OF_PAY                                 as                                 INDM_SCH_NO_OF_PAY 
		, INDM_SCH_ID                                        as                                        INDM_SCH_ID 
		, INDM_PAY_ID                                        as                                        INDM_PAY_ID 
		from SRC_ISS
            ),
LOGIC_ISD as ( SELECT 
		  INDM_SCH_DTL_FNL_PAY_IND                           as                           INDM_SCH_DTL_FNL_PAY_IND 
		, VOID_IND                                           as                                           VOID_IND 
		, INDM_SCH_DTL_DRV_EFF_DT                            as                            INDM_SCH_DTL_DRV_EFF_DT 
		, INDM_SCH_DTL_DRV_END_DT                            as                            INDM_SCH_DTL_DRV_END_DT 
		, INDM_SCH_DLVR_DT                                   as                                   INDM_SCH_DLVR_DT 
		, INDM_SCH_DTL_PRCS_DT                               as                               INDM_SCH_DTL_PRCS_DT 
		, INDM_SCH_DTL_DRV_DD                                as                                INDM_SCH_DTL_DRV_DD 
		, INDM_SCH_DTL_DRV_AMT                               as                               INDM_SCH_DTL_DRV_AMT 
		, INDM_SCH_DTL_OFST_AMT                              as                              INDM_SCH_DTL_OFST_AMT 
		, INDM_SCH_DTL_LMP_RED_AMT                           as                           INDM_SCH_DTL_LMP_RED_AMT 
		, INDM_SCH_DTL_OTHR_RED_AMT                          as                          INDM_SCH_DTL_OTHR_RED_AMT 
		, INDM_SCH_DTL_DRV_NET_AMT                           as                           INDM_SCH_DTL_DRV_NET_AMT 
		, INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID 
		, INDM_SCH_ID                                        as                                        INDM_SCH_ID 
		from SRC_ISD
            ),
LOGIC_U as ( SELECT 
		  TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		, USER_ID                                            as                                            USER_ID 
		from SRC_U
            ),
LOGIC_AU as ( SELECT 
		  TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		, USER_ID                                            as                                            USER_ID 
		from SRC_U
            ),
LOGIC_CFT_PAY as ( SELECT 
          CFT_ID                                             as                                             CFT_ID 
		from SRC_CFT_PAY
            ),
LOGIC_CCS as ( SELECT 
		  TRIM( CUST_CHLD_SUPT_CASE_NO )                     as                             CUST_CHLD_SUPT_CASE_NO 
		, CUST_CHLD_SUPT_WH_AMT                              as                              CUST_CHLD_SUPT_WH_AMT 
		, CUST_CHLD_SUPT_VOID_IND                            as                            CUST_CHLD_SUPT_VOID_IND 
		, CUST_CHLD_SUPT_ID                                  as                                  CUST_CHLD_SUPT_ID 
		from SRC_CCS
            ),
			
LOGIC_CALC as ( SELECT 
		  CALC_RSLT_ID 
		, SUM(CASE WHEN CALC_RSLT_ATTR_TYP_NM ='SCHEDULED IMPAIRMENT AWARD LOSS OF USE PERCENT'
                  THEN CALC_RSLT_ATTR_TEXT_VAL END) PP_LOSS_PRCNT
		, LISTAGG( distinct CALC_RSLT_ATTR_GRP_SEQ_NO, ', ') within group(order by CALC_RSLT_ATTR_GRP_SEQ_NO)  CALC_RSLT_ATTR_GRP_SEQ_NO
		, nullif(LISTAGG( distinct (CASE WHEN CALC_RSLT_ATTR_TYP_NM = 'SCHEDULED IMPAIRMENT AWARD INJURY TYPE CODE'
                THEN CALC_RSLT_ATTR_TEXT_VAL END), ', '),'')  SCH_AWARDS_INJURY_TYP_CODE
		, nullif(LISTAGG(distinct CASE WHEN CALC_RSLT_ATTR_TYP_NM = 'SCHEDULED IMPAIRMENT AWARD INJURY TYPE NAME'
                 THEN CALC_RSLT_ATTR_TEXT_VAL END, ', '),'') SCH_AWARDS_INJURY_TYP_NAME		
		from SRC_CALC
		 WHERE CALC_RSLT_ATTR_GRP_TYP_NM = 'SCHEDULED IMPAIRMENT AWARDS' AND CALC_RSLT_ATTR_TYP_NM 
			IN ('SCHEDULED IMPAIRMENT AWARD LOSS OF USE PERCENT', 'SCHEDULED IMPAIRMENT AWARD INJURY TYPE CODE', 'SCHEDULED IMPAIRMENT AWARD INJURY TYPE NAME')
            GROUP BY 1 ORDER BY 1
   
			),
LOGIC_CFT as ( SELECT 
		  FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID 
		, CFT_DRV_BAL_AMT                                    as                                    CFT_DRV_BAL_AMT 
		, CFT_ID                                             as                                             CFT_ID 
		from SRC_CFT_PAY
        WHERE FNCL_TRAN_TYP_ID = 152 AND CFT_DRV_BAL_AMT < 0 
            ),		
LOGIC_CFT_DSPT as ( SELECT 
		  AGRE_ID                                            as                                            AGRE_ID 
		, min(CFT_DT)                                        as                                             CFT_DT 
		from SRC_CFT_PAY
            WHERE FNCL_TRAN_TYP_NM = 'WRITE-OFF TO DISPUTE' AND CFT_ID_RVRS_ID IS NULL
			group by 1),				
LOGIC_CFTA as ( SELECT 
		  FTAT_ID                                            as                                            FTAT_ID 
		, CFT_ID_RVRS                                        as                                        CFT_ID_RVRS 
		, CFTA_AMT                                           as                                           CFTA_AMT 
		, CFT_ID_APLD_TO                                     as                                     CFT_ID_APLD_TO 
		, CFT_ID_APLD_FR                                     as                                     CFT_ID_APLD_FR      
		from SRC_CFTA
         WHERE FTAT_ID = 6311563 AND CFT_ID_RVRS IS NULL AND CFTA_AMT <> 0  
            ),
	 
LOGIC_HIST as ( SELECT 
		  TRIM( INDM_SCH_DTL_STS_TYP_CD )                    as                            INDM_SCH_DTL_STS_TYP_CD 
		, VOID_IND                                           as                                           VOID_IND 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID 
        , COALESCE(AUDIT_USER_ID_UPDT, AUDIT_USER_ID_CREA)   as                                 CLAIM_AUTH_USER_ID     
		from SRC_HIST
         where INDM_SCH_DTL_STS_TYP_CD in ('REL', 'PAID') AND VOID_IND='N' 
		 QUALIFY (ROW_NUMBER() OVER (PARTITION BY INDM_SCH_DTL_ID ORDER BY  INDM_SCH_DTL_STS_TYP_CD DESC, HIST_EFF_DTM DESC )) =1
		 ),	

LOGIC_ISDC as ( SELECT 
		  VOID_IND                                           as                                           VOID_IND 
		, CUST_CHLD_SUPT_ID                                  as                                  CUST_CHLD_SUPT_ID 
		, INDM_SCH_DTL_AMT_ID                                as                                INDM_SCH_DTL_AMT_ID 
		from SRC_ISDC
            ),
LOGIC_PR as ( SELECT 
		  TRIM( PAY_MEDA_PREF_TYP_CD )                       as                               PAY_MEDA_PREF_TYP_CD 
		, TRIM( PAY_REQS_TYP_CD )                            as                                    PAY_REQS_TYP_CD 
        , PAY_REQS_DT                                        as                                        PAY_REQS_DT 
	    , PAY_REQS_ID                                        as                                        PAY_REQS_ID 
		 ,PAY_REQS_NO                                        as                                        PAY_REQS_NO
		from SRC_PR
            ),
LOGIC_PRS as ( SELECT 
          TRIM( PAY_REQS_STT_TYP_CD )                        as                                PAY_REQS_STT_TYP_CD 
		, TRIM( PAY_REQS_STS_TYP_CD )                        as                                PAY_REQS_STS_TYP_CD 
		, TRIM( PAY_REQS_STS_RSN_TYP_CD )                    as                            PAY_REQS_STS_RSN_TYP_CD     
		, VOID_IND                                           as                                           VOID_IND 
		, PAY_REQS_STS_END_DT                                as                                PAY_REQS_STS_END_DT 
		, PAY_REQS_ID                                        as                                        PAY_REQS_ID 
        , PAY_REQS_STS_EFF_DT as PAY_REQS_STS_EFF_DT
              from SRC_PRS 
             WHERE VOID_IND = 'N' AND PAY_REQS_STS_END_DT IS NULL 
            ),
LOGIC_XREF as ( SELECT 
		  VOID_IND                                           as                                           VOID_IND 
		, PAY_REQS_ID                                        as                                        PAY_REQS_ID 
		, CFT_ID                                             as                                             CFT_ID 
		from SRC_XREF
            )

---- RENAME LAYER ----
,

RENAME_IP as ( SELECT 
		  INDM_PAY_ID                                        as                                        INDM_PAY_ID
		, CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                            AGRE_ID
		, BNFT_TYP_CD                                        as                                        BNFT_TYP_CD
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD
		, BNFT_RPT_TYP_CD                                    as                                    BNFT_RPT_TYP_CD
		, BNFT_RPT_TYP_NM                                    as                                    BNFT_RPT_TYP_NM
		, INDM_PAY_EFF_DT                                    as                                    INDM_PAY_EFF_DT
		, INDM_PAY_END_DT                                    as                                    INDM_PAY_END_DT
		, INDM_RSN_TYP_CD                                    as                                    INDM_RSN_TYP_CD
		, INDM_PAY_RECALC_IND                                as                                INDM_PAY_RECALC_IND
		, VOID_IND                                           as                                        IP_VOID_IND
		, INDM_PAY_DRV_TOT_AMT                               as                               INDM_PAY_DRV_TOT_AMT
		, INDM_PAY_DRV_SCH_TOT_AMT                           as                           INDM_PAY_DRV_SCH_TOT_AMT
		, INDM_PAY_DRV_WEK                                   as                                   INDM_PAY_DRV_WEK
		, INDM_PAY_DRV_DD                                    as                                    INDM_PAY_DRV_DD
		, BNFT_TYP_NM                                        as                                        BNFT_TYP_NM
	--	, INDM_PAY_ID                                        as                                        INDM_PAY_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, CALC_RSLT_ID                                       as                                       CALC_RSLT_ID 
		, BNFT_CTG_TYP_CD                                    as                                    BNFT_CTG_TYP_CD
		, BNFT_CTG_TYP_NM                                    as                                    BNFT_CTG_TYP_NM
		, JUR_TYP_NM                                         as                                         JUR_TYP_NM		
				FROM     LOGIC_IP   ), 
RENAME_ISDA as ( SELECT 
		  INDM_SCH_DTL_AMT_ID                                as                                INDM_SCH_DTL_AMT_ID
		, INDM_SCH_DTL_AMT_CHK_GRP_NO                        as                        INDM_SCH_DTL_AMT_CHK_GRP_NO
		, INDM_SCH_DTL_AMT_TYP_CD                            as                            INDM_SCH_DTL_AMT_TYP_CD
		, INDM_SCH_DTL_STS_TYP_CD                            as                            INDM_SCH_DTL_STS_TYP_CD
		, INDM_SCH_DTL_AMT_PRI_IND                           as                           INDM_SCH_DTL_AMT_PRI_IND
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                        INDM_SCH_DTL_AMT_MAILTO_IND
		, INDM_SCH_DTL_AMT_RMND_IND                          as                          INDM_SCH_DTL_AMT_RMND_IND
		, VOID_IND                                           as                                      ISDA_VOID_IND
		, INDM_SCH_DTL_AMT_AMT                               as                               INDM_SCH_DTL_AMT_AMT
--		, INDM_SCH_DTL_AMT_TYP_CD                            as                            INDM_SCH_DTL_AMT_TYP_CD
		, CFT_ID                                             as                                             CFT_ID
	--	, INDM_SCH_DTL_AMT_ID                                as                                INDM_SCH_DTL_AMT_ID
		, CUST_ID                                            as                                            CUST_ID
		, INDM_SCH_DTL_ID                                    as                               ISDA_INDM_SCH_DTL_ID 
				FROM     LOGIC_ISDA   ), 
RENAME_CP as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND
--		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND
		, CP_VOID_IND                                        as                                        CP_VOID_IND
--		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, CLM_PTCP_END_DT                                    as                                    CLM_PTCP_END_DT
		, CUST_ID                                            as                                         CP_CUST_ID
		, AGRE_ID                                            as                                         CP_AGRE_ID 
				FROM     LOGIC_CP   ), 
RENAME_ISS as ( SELECT 
		  INDM_FREQ_TYP_CD                                   as                                   INDM_FREQ_TYP_CD
		, INDM_SCH_AUTO_PAY_IND                              as                              INDM_SCH_AUTO_PAY_IND
		, VOID_IND                                           as                                       ISS_VOID_IND
		, INDM_SCH_FST_PRCS_DT                               as                               INDM_SCH_FST_PRCS_DT
		, INDM_SCH_NO_OF_PAY                                 as                                 INDM_SCH_NO_OF_PAY
		, INDM_SCH_ID                                        as                                        INDM_SCH_ID
		, INDM_PAY_ID                                        as                                    ISS_INDM_PAY_ID 
				FROM     LOGIC_ISS   ), 
RENAME_ISD as ( SELECT 
		  INDM_SCH_DTL_FNL_PAY_IND                           as                           INDM_SCH_DTL_FNL_PAY_IND
		, VOID_IND                                           as                                       ISD_VOID_IND
		, INDM_SCH_DTL_DRV_EFF_DT                            as                            INDM_SCH_DTL_DRV_EFF_DT
		, INDM_SCH_DTL_DRV_END_DT                            as                            INDM_SCH_DTL_DRV_END_DT
		, INDM_SCH_DLVR_DT                                   as                                   INDM_SCH_DLVR_DT
		, INDM_SCH_DTL_PRCS_DT                               as                               INDM_SCH_DTL_PRCS_DT
		, INDM_SCH_DTL_DRV_DD                                as                                INDM_SCH_DTL_DRV_DD
		, INDM_SCH_DTL_DRV_AMT                               as                               INDM_SCH_DTL_DRV_AMT
		, INDM_SCH_DTL_OFST_AMT                              as                              INDM_SCH_DTL_OFST_AMT
		, INDM_SCH_DTL_LMP_RED_AMT                           as                           INDM_SCH_DTL_LMP_RED_AMT
		, INDM_SCH_DTL_OTHR_RED_AMT                          as                          INDM_SCH_DTL_OTHR_RED_AMT
		, INDM_SCH_DTL_DRV_NET_AMT                           as                           INDM_SCH_DTL_DRV_NET_AMT
		, INDM_SCH_DTL_ID                                    as                                    INDM_SCH_DTL_ID
		, INDM_SCH_ID                                        as                                    ISD_INDM_SCH_ID 
				FROM     LOGIC_ISD   ), 
RENAME_U as ( SELECT 
		  USER_LGN_NM                                        as                      INDMN_PLAN_CREATE_USER_LGN_NM
		, USER_ID                                            as                                          U_USER_ID 
				FROM     LOGIC_U   ), 
RENAME_AU as ( SELECT 
		  USER_LGN_NM                                        as                             CLAIM_AUTH_USER_LGN_NM
		, USER_ID                                            as                                         AU_USER_ID 
				FROM     LOGIC_AU   ), 
RENAME_CFT_PAY as ( SELECT 

		  CFT_ID                                             as                                     CFT_PAY_CFT_ID 
				FROM     LOGIC_CFT_PAY   ), 
RENAME_CCS as ( SELECT 
		  CUST_CHLD_SUPT_CASE_NO                             as                             CUST_CHLD_SUPT_CASE_NO
		, CUST_CHLD_SUPT_WH_AMT                              as                              CUST_CHLD_SUPT_WH_AMT
		, CUST_CHLD_SUPT_VOID_IND                            as                            CUST_CHLD_SUPT_VOID_IND
		, CUST_CHLD_SUPT_ID                                  as                              CCS_CUST_CHLD_SUPT_ID 
				FROM     LOGIC_CCS   ), 
RENAME_CALC as ( SELECT 
		  CALC_RSLT_ID                                       as                                  CALC_CALC_RSLT_ID 
        , PP_LOSS_PRCNT                                      as                                      PP_LOSS_PRCNT
		, CALC_RSLT_ATTR_GRP_SEQ_NO                          as                          CALC_RSLT_ATTR_GRP_SEQ_NO
        , SCH_AWARDS_INJURY_TYP_CODE                         as                         SCH_AWARDS_INJURY_TYP_CODE
        , SCH_AWARDS_INJURY_TYP_NAME                         as                         SCH_AWARDS_INJURY_TYP_NAME

				FROM     LOGIC_CALC   ), 
RENAME_CFT as ( SELECT 
		  FNCL_TRAN_TYP_ID                                   as                               CFT_FNCL_TRAN_TYP_ID
		, CFT_DRV_BAL_AMT                                    as                                CFT_CFT_DRV_BAL_AMT
		, CFT_ID                                             as                                         CFT_CFT_ID 
				FROM     LOGIC_CFT   ), 
RENAME_CFT_DSPT as ( SELECT 
		  AGRE_ID                                            as                                   CFT_DSPT_AGRE_ID
		, CFT_DT                                             as                                    CFT_DSPT_CFT_DT 
				FROM     LOGIC_CFT_DSPT   ), 
RENAME_CFTA as ( SELECT 
		  FTAT_ID                                            as                                            FTAT_ID
		, CFT_ID_RVRS                                        as                                        CFT_ID_RVRS
		, CFTA_AMT                                           as                                           CFTA_AMT
		, CFT_ID_APLD_TO                                     as                                     CFT_ID_APLD_TO
		, CFT_ID_APLD_FR                                     as                                     CFT_ID_APLD_FR 
        -- , CFT_PAY_CFT_ID                                     as                                     CFT_PAY_CFT_ID       
                FROM     LOGIC_CFTA   ), 
RENAME_HIST as ( SELECT 
		  INDM_SCH_DTL_STS_TYP_CD                            as                       HIST_INDM_SCH_DTL_STS_TYP_CD
		, VOID_IND                                           as                                      HIST_VOID_IND
		, AUDIT_USER_ID_CREA                                 as                            HIST_AUDIT_USER_ID_CREA
		, AUDIT_USER_ID_UPDT                                 as                            HIST_AUDIT_USER_ID_UPDT
		, INDM_SCH_DTL_ID                                    as                               HIST_INDM_SCH_DTL_ID 
        , CLAIM_AUTH_USER_ID                                 as                                 CLAIM_AUTH_USER_ID
				FROM     LOGIC_HIST   ), 
RENAME_ISDC as ( SELECT 
		  VOID_IND                                           as                                      ISDC_VOID_IND
		, CUST_CHLD_SUPT_ID                                  as                                  CUST_CHLD_SUPT_ID
		, INDM_SCH_DTL_AMT_ID                                as                           ISDC_INDM_SCH_DTL_AMT_ID 
				FROM     LOGIC_ISDC   ), 
RENAME_PR as ( SELECT 
          PAY_MEDA_PREF_TYP_CD                               as                               PAY_MEDA_PREF_TYP_CD
		, PAY_REQS_TYP_CD                                    as                                    PAY_REQS_TYP_CD
         ,PAY_REQS_DT                                         as                                        PAY_REQS_DT 
        -- ,  PAY_MEDA_PREF_TYP_CD                               as                               PAY_MEDA_PREF_TYP_CD    
		 , PAY_REQS_ID                                        as                                     PR_PAY_REQS_ID 
		 ,PAY_REQS_NO                                        as                                        PAY_REQS_NO		 
				FROM     LOGIC_PR   ), 
RENAME_PRS as ( SELECT 
       	  PAY_REQS_STT_TYP_CD                                as                                PAY_REQS_STT_TYP_CD
		, PAY_REQS_STS_TYP_CD                                as                                PAY_REQS_STS_TYP_CD
		, PAY_REQS_STS_RSN_TYP_CD                            as                            PAY_REQS_STS_RSN_TYP_CD                      
		, VOID_IND                                           as                                       PRS_VOID_IND
		, PAY_REQS_STS_END_DT                                as                                PAY_REQS_STS_END_DT
		, PAY_REQS_ID                                        as                                    PRS_PAY_REQS_ID 
         ,PAY_REQS_STS_EFF_DT as PAY_REQS_STS_EFF_DT
               FROM     LOGIC_PRS   ), 
RENAME_XREF as ( SELECT 
		  VOID_IND                                           as                                      XREF_VOID_IND
		, PAY_REQS_ID                                        as                                        PAY_REQS_ID
		, CFT_ID                                             as                                        XREF_CFT_ID 
				FROM     LOGIC_XREF   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_IP                             as ( SELECT * from    RENAME_IP   ),
FILTER_ISS                            as ( SELECT * from    RENAME_ISS   ),
FILTER_U                              as ( SELECT * from    RENAME_U   ),
FILTER_CALC                           as ( SELECT * from    RENAME_CALC
											),
FILTER_CFT_DSPT                       as ( SELECT * from    RENAME_CFT_DSPT 
                                              ),
FILTER_ISD                            as ( SELECT * from    RENAME_ISD   ),
FILTER_HIST                           as ( SELECT * from    RENAME_HIST 
                                             ),
FILTER_ISDA                           as ( SELECT * from    RENAME_ISDA   ),
FILTER_CP                             as ( SELECT * from    RENAME_CP   ),
FILTER_CFT                            as ( SELECT * from    RENAME_CFT  ),
FILTER_ISDC                           as ( SELECT * from    RENAME_ISDC 
                                            WHERE ISDC_VOID_IND = 'N'  ),
FILTER_CCS                            as ( SELECT * from    RENAME_CCS 
                                            WHERE CUST_CHLD_SUPT_VOID_IND = 'N'  ),
FILTER_AU                             as ( SELECT * from    RENAME_AU   ),
FILTER_CFT_PAY                        as ( SELECT * from    RENAME_CFT_PAY 
                                         ),
											
FILTER_CFTA                           as ( SELECT * from    RENAME_CFTA ),
FILTER_XREF                           as ( SELECT * from    RENAME_XREF 
                                            WHERE XREF_VOID_IND = 'N'  ),
FILTER_PR                             as ( SELECT * from    RENAME_PR   ),
FILTER_PRS                            as ( SELECT * from    RENAME_PRS )
                                               
                                            ,

---- JOIN LAYER ----

ETL_SUB AS (

Select distinct FILTER_CFT_PAY.CFT_PAY_CFT_ID, FILTER_PR.PAY_MEDA_PREF_TYP_CD, FILTER_PR.PAY_REQS_TYP_CD, 
  FILTER_PRS.PAY_REQS_STT_TYP_CD, 
  FILTER_PRS.PAY_REQS_STS_TYP_CD, 
  FILTER_PRS.PAY_REQS_STS_RSN_TYP_CD,
  FILTER_PR.PAY_REQS_NO,
FILTER_PR.PAY_REQS_DT
from  FILTER_CFT_PAY
INNER JOIN  FILTER_CFTA ON FILTER_CFT_PAY.CFT_PAY_CFT_ID = FILTER_CFTA.CFT_ID_APLD_FR  AND FILTER_CFTA.FTAT_ID = 6311563 AND FILTER_CFTA.CFT_ID_RVRS IS NULL AND FILTER_CFTA.CFTA_AMT <> 0
INNER JOIN  FILTER_XREF ON FILTER_CFTA.CFT_ID_APLD_TO = FILTER_XREF.XREF_CFT_ID  AND FILTER_XREF.XREF_VOID_IND = 'N'
INNER JOIN  FILTER_PR   ON FILTER_XREF.PAY_REQS_ID = FILTER_PR.PR_PAY_REQS_ID 
INNER JOIN  FILTER_PRS  ON FILTER_PR.PR_PAY_REQS_ID=FILTER_PRS.PRS_PAY_REQS_ID AND FILTER_PRS.PRS_VOID_IND = 'N'AND FILTER_PRS.PAY_REQS_STS_END_DT IS NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY FILTER_CFT_PAY.CFT_PAY_CFT_ID ORDER BY FILTER_PR.PAY_REQS_DT DESC, FILTER_PRS.PAY_REQS_STS_EFF_DT DESC, FILTER_PR.PR_PAY_REQS_ID DESC))=1 

),

IP as (
select  * from FILTER_IP
LEFT JOIN FILTER_ISS ON FILTER_IP.INDM_PAY_ID=FILTER_ISS.ISS_INDM_PAY_ID 
LEFT JOIN FILTER_U ON FILTER_IP.AUDIT_USER_ID_CREA=FILTER_U.U_USER_ID
LEFT JOIN FILTER_CALC ON  FILTER_IP.CALC_RSLT_ID=FILTER_CALC.CALC_CALC_RSLT_ID and FILTER_IP.BNFT_TYP_NM = '%PP' 
LEFT JOIN FILTER_CFT_DSPT ON FILTER_IP.AGRE_ID=FILTER_CFT_DSPT.CFT_DSPT_AGRE_ID
LEFT JOIN FILTER_ISD ON FILTER_ISS.INDM_SCH_ID=FILTER_ISD.ISD_INDM_SCH_ID
LEFT JOIN FILTER_HIST ON FILTER_ISD.INDM_SCH_DTL_ID=FILTER_HIST.HIST_INDM_SCH_DTL_ID
LEFT JOIN FILTER_ISDA ON FILTER_ISD.INDM_SCH_DTL_ID=FILTER_ISDA.ISDA_INDM_SCH_DTL_ID
LEFT JOIN FILTER_CP ON FILTER_IP.AGRE_ID=FILTER_CP.CP_AGRE_ID and  FILTER_ISDA.CUST_ID=FILTER_CP.CP_CUST_ID
LEFT JOIN FILTER_CFT ON FILTER_ISDA.CFT_ID=FILTER_CFT.CFT_CFT_ID
LEFT JOIN FILTER_ISDC ON FILTER_ISDA.INDM_SCH_DTL_AMT_ID=FILTER_ISDC.ISDC_INDM_SCH_DTL_AMT_ID
LEFT JOIN FILTER_CCS ON FILTER_ISDC.CUST_CHLD_SUPT_ID=FILTER_CCS.CCS_CUST_CHLD_SUPT_ID
LEFT JOIN FILTER_AU ON FILTER_HIST.CLAIM_AUTH_USER_ID=FILTER_AU.AU_USER_ID
LEFT JOIN ETL_SUB on FILTER_ISDA.CFT_ID=ETL_SUB.CFT_PAY_CFT_ID and FILTER_ISDA.INDM_SCH_DTL_AMT_TYP_CD = 'BNFT' )
								
								
select  
  md5(cast(
    
    coalesce(cast(INDM_PAY_ID as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_AMT_ID as 
    varchar
), '') || '-' || coalesce(cast(AGRE_ID as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY 
, INDM_PAY_ID
, INDM_SCH_DTL_AMT_ID
, CLM_NO
, AGRE_ID
, BNFT_TYP_CD
, BNFT_TYP_NM
, BNFT_CTG_TYP_CD
, BNFT_CTG_TYP_NM
, JUR_TYP_CD
, JUR_TYP_NM
, BNFT_RPT_TYP_CD
, BNFT_RPT_TYP_NM
, CUST_NO
, PTCP_TYP_CD
, CLM_PTCP_PRI_IND
, INDM_SCH_DTL_AMT_CHK_GRP_NO
, INDM_PAY_EFF_DT
, INDM_PAY_END_DT
, INDM_FREQ_TYP_CD
, INDM_RSN_TYP_CD
, INDM_SCH_DTL_AMT_TYP_CD
, INDM_SCH_DTL_STS_TYP_CD
, INDM_SCH_DTL_FNL_PAY_IND
, INDM_SCH_AUTO_PAY_IND
, INDM_PAY_RECALC_IND
, INDM_SCH_DTL_AMT_PRI_IND
, INDM_SCH_DTL_AMT_MAILTO_IND
, INDM_SCH_DTL_AMT_RMND_IND
, NVL2(CFT_CFT_ID,'Y','N') AS OVR_PYMNT_BAL_IND
, IP_VOID_IND
, ISS_VOID_IND
, ISD_VOID_IND
, ISDA_VOID_IND
, INDMN_PLAN_CREATE_USER_LGN_NM
, CLAIM_AUTH_USER_LGN_NM
, INDM_SCH_FST_PRCS_DT
, INDM_SCH_DTL_DRV_EFF_DT
, INDM_SCH_DTL_DRV_END_DT
, INDM_SCH_DLVR_DT
, INDM_SCH_DTL_PRCS_DT
, PAY_MEDA_PREF_TYP_CD
, PAY_REQS_TYP_CD
, PAY_REQS_STT_TYP_CD
, PAY_REQS_STS_TYP_CD
, PAY_REQS_STS_RSN_TYP_CD
, PAY_REQS_NO
, PAY_REQS_DT
, CUST_CHLD_SUPT_CASE_NO
, CUST_CHLD_SUPT_WH_AMT
, INDM_PAY_DRV_TOT_AMT
, INDM_PAY_DRV_SCH_TOT_AMT
, INDM_SCH_NO_OF_PAY
, INDM_SCH_DTL_DRV_DD
, INDM_SCH_DTL_DRV_AMT
, INDM_SCH_DTL_OFST_AMT
, INDM_SCH_DTL_LMP_RED_AMT
, INDM_SCH_DTL_OTHR_RED_AMT
, INDM_SCH_DTL_DRV_NET_AMT
, INDM_SCH_DTL_AMT_AMT
, PP_LOSS_PRCNT
, NULLIF(TRIM(SCH_AWARDS_INJURY_TYP_CODE), '') AS SCH_AWARDS_INJURY_TYP_CODE
, NULLIF(TRIM(SCH_AWARDS_INJURY_TYP_NAME), '') AS SCH_AWARDS_INJURY_TYP_NAME
, INDM_PAY_DRV_WEK
, INDM_PAY_DRV_DD
, DATEDIFF(DAYS, CFT_DSPT_CFT_DT,  CURRENT_DATE ) AS  DISPUTED_TRANSACTION_DAY_COUNT	
from IP
      );
    