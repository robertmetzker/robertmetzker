
  create or replace  view DEV_EDW.STAGING.DSV_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT  as (
    

---- SRC LAYER ----
WITH
SRC_IP as ( SELECT *     from     STAGING.DST_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT ),
//SRC_IP as ( SELECT *     from     DST_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT) ,

---- LOGIC LAYER ----

LOGIC_IP as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, INDM_PAY_ID                                        as                                        INDM_PAY_ID 
		, INDM_SCH_DTL_AMT_ID                                as                                INDM_SCH_DTL_AMT_ID 
		, CLM_NO                                             as                                             CLM_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, BNFT_TYP_CD                                        as                                        BNFT_TYP_CD 
		, BNFT_TYP_NM                                        as                                        BNFT_TYP_NM 
		, BNFT_CTG_TYP_CD                                    as                                    BNFT_CTG_TYP_CD 
		, BNFT_CTG_TYP_NM                                    as                                    BNFT_CTG_TYP_NM 
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD 
		, JUR_TYP_NM                                         as                                         JUR_TYP_NM 
		, BNFT_RPT_TYP_CD                                    as                                    BNFT_RPT_TYP_CD 
		, BNFT_RPT_TYP_NM                                    as                                    BNFT_RPT_TYP_NM 
		, CUST_NO                                            as                                            CUST_NO 
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD 
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND 
		, INDM_SCH_DTL_AMT_CHK_GRP_NO                        as                        INDM_SCH_DTL_AMT_CHK_GRP_NO 
		, INDM_PAY_EFF_DT                                    as                                    INDM_PAY_EFF_DT 
		, INDM_PAY_END_DT                                    as                                    INDM_PAY_END_DT 
		, INDM_FREQ_TYP_CD                                   as                                   INDM_FREQ_TYP_CD 
		, INDM_RSN_TYP_CD                                    as                                    INDM_RSN_TYP_CD 
		, INDM_SCH_DTL_AMT_TYP_CD                            as                            INDM_SCH_DTL_AMT_TYP_CD 
		, INDM_SCH_DTL_STS_TYP_CD                            as                            INDM_SCH_DTL_STS_TYP_CD 
		, INDM_SCH_DTL_FNL_PAY_IND                           as                           INDM_SCH_DTL_FNL_PAY_IND 
		, INDM_SCH_AUTO_PAY_IND                              as                              INDM_SCH_AUTO_PAY_IND 
		, INDM_PAY_RECALC_IND                                as                                INDM_PAY_RECALC_IND 
		, INDM_SCH_DTL_AMT_PRI_IND                           as                           INDM_SCH_DTL_AMT_PRI_IND 
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                        INDM_SCH_DTL_AMT_MAILTO_IND 
		, INDM_SCH_DTL_AMT_RMND_IND                          as                          INDM_SCH_DTL_AMT_RMND_IND 
		, OVR_PYMNT_BAL_IND                                  as                                  OVR_PYMNT_BAL_IND 
		, IP_VOID_IND                                        as                                        IP_VOID_IND 
		, ISS_VOID_IND                                       as                                       ISS_VOID_IND 
		, ISD_VOID_IND                                       as                                       ISD_VOID_IND 
		, ISDA_VOID_IND                                      as                                      ISDA_VOID_IND 
		, INDMN_PLAN_CREATE_USER_LGN_NM                      as                      INDMN_PLAN_CREATE_USER_LGN_NM 
		, CLAIM_AUTH_USER_LGN_NM                             as                             CLAIM_AUTH_USER_LGN_NM 
		, INDM_SCH_FST_PRCS_DT                               as                               INDM_SCH_FST_PRCS_DT 
		, INDM_SCH_DTL_DRV_EFF_DT                            as                            INDM_SCH_DTL_DRV_EFF_DT 
		, INDM_SCH_DTL_DRV_END_DT                            as                            INDM_SCH_DTL_DRV_END_DT 
		, INDM_SCH_DLVR_DT                                   as                                   INDM_SCH_DLVR_DT 
		, INDM_SCH_DTL_PRCS_DT                               as                               INDM_SCH_DTL_PRCS_DT 
		, PAY_MEDA_PREF_TYP_CD                               as                               PAY_MEDA_PREF_TYP_CD 
		, PAY_REQS_TYP_CD                                    as                                    PAY_REQS_TYP_CD 
		, PAY_REQS_STT_TYP_CD                                as                                PAY_REQS_STT_TYP_CD 
		, PAY_REQS_STS_TYP_CD                                as                                PAY_REQS_STS_TYP_CD 
		, PAY_REQS_STS_RSN_TYP_CD                            as                            PAY_REQS_STS_RSN_TYP_CD 
		, PAY_REQS_DT                                        as                                        PAY_REQS_DT 
		, PAY_REQS_NO                                        as                                        PAY_REQS_NO 
		, CUST_CHLD_SUPT_CASE_NO                             as                             CUST_CHLD_SUPT_CASE_NO 
		, CUST_CHLD_SUPT_WH_AMT                              as                              CUST_CHLD_SUPT_WH_AMT 
		, INDM_PAY_DRV_TOT_AMT                               as                               INDM_PAY_DRV_TOT_AMT 
		, INDM_PAY_DRV_SCH_TOT_AMT                           as                           INDM_PAY_DRV_SCH_TOT_AMT 
		, INDM_SCH_NO_OF_PAY                                 as                                 INDM_SCH_NO_OF_PAY 
		, INDM_SCH_DTL_DRV_DD                                as                                INDM_SCH_DTL_DRV_DD 
		, INDM_SCH_DTL_DRV_AMT                               as                               INDM_SCH_DTL_DRV_AMT 
		, INDM_SCH_DTL_OFST_AMT                              as                              INDM_SCH_DTL_OFST_AMT 
		, INDM_SCH_DTL_LMP_RED_AMT                           as                           INDM_SCH_DTL_LMP_RED_AMT 
		, INDM_SCH_DTL_OTHR_RED_AMT                          as                          INDM_SCH_DTL_OTHR_RED_AMT 
		, INDM_SCH_DTL_DRV_NET_AMT                           as                           INDM_SCH_DTL_DRV_NET_AMT 
		, INDM_SCH_DTL_AMT_AMT                               as                               INDM_SCH_DTL_AMT_AMT 
		, PP_LOSS_PRCNT                                      as                                      PP_LOSS_PRCNT 
		, SCH_AWARDS_INJURY_TYP_CODE                         as                         SCH_AWARDS_INJURY_TYP_CODE 
		, SCH_AWARDS_INJURY_TYP_NAME                         as                         SCH_AWARDS_INJURY_TYP_NAME 
		, INDM_PAY_DRV_WEK                                   as                                   INDM_PAY_DRV_WEK 
		, INDM_PAY_DRV_DD                                    as                                    INDM_PAY_DRV_DD 
		, DISPUTED_TRANSACTION_DAY_COUNT                     as                     DISPUTED_TRANSACTION_DAY_COUNT 
		from SRC_IP
            )

---- RENAME LAYER ----
,

RENAME_IP as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, INDM_PAY_ID                                        as                                  INDEMNITY_PLAN_ID
		, INDM_SCH_DTL_AMT_ID                                as               INDEMNITY_SCHEDULE_PAYMENT_DETAIL_ID
		, CLM_NO                                             as                                       CLAIM_NUMBER
		, AGRE_ID                                            as                                            AGRE_ID
		, BNFT_TYP_CD                                        as                                  BENEFIT_TYPE_CODE
		, BNFT_TYP_NM                                        as                                  BENEFIT_TYPE_DESC
		, BNFT_CTG_TYP_CD                                    as                         BENEFIT_CATEGORY_TYPE_CODE
		, BNFT_CTG_TYP_NM                                    as                         BENEFIT_CATEGORY_TYPE_DESC
		, JUR_TYP_CD                                         as                             JURISDICTION_TYPE_CODE
		, JUR_TYP_NM                                         as                             JURISDICTION_TYPE_DESC
		, BNFT_RPT_TYP_CD                                    as                                  BNFT_RPT_TYP_CODE
		, BNFT_RPT_TYP_NM                                    as                        BENEFIT_REPORTING_TYPE_DESC
		, CUST_NO                                            as                                            CUST_NO
		, PTCP_TYP_CD                                        as                                      PTCP_TYP_CODE
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND
		, INDM_SCH_DTL_AMT_CHK_GRP_NO                        as                           PAYEE_CHECK_GROUP_NUMBER
		, INDM_PAY_EFF_DT                                    as                                  INDM_PAY_EFF_DATE
		, INDM_PAY_END_DT                                    as                                  INDM_PAY_END_DATE
		, INDM_FREQ_TYP_CD                                   as                                 INDM_FREQ_TYP_CODE
		, INDM_RSN_TYP_CD                                    as                                  INDM_RSN_TYP_CODE
		, INDM_SCH_DTL_AMT_TYP_CD                            as                          INDM_SCH_DTL_AMT_TYP_CODE
		, INDM_SCH_DTL_STS_TYP_CD                            as                          INDM_SCH_DTL_STS_TYP_CODE
		, INDM_SCH_DTL_FNL_PAY_IND                           as                           INDM_SCH_DTL_FNL_PAY_IND
		, INDM_SCH_AUTO_PAY_IND                              as                              INDM_SCH_AUTO_PAY_IND
		, INDM_PAY_RECALC_IND                                as                                INDM_PAY_RECALC_IND
		, INDM_SCH_DTL_AMT_PRI_IND                           as                           INDM_SCH_DTL_AMT_PRI_IND
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                        INDM_SCH_DTL_AMT_MAILTO_IND
		, INDM_SCH_DTL_AMT_RMND_IND                          as                          INDM_SCH_DTL_AMT_RMND_IND
		, OVR_PYMNT_BAL_IND                                  as                                  OVR_PYMNT_BAL_IND
		, IP_VOID_IND                                        as                                        IP_VOID_IND
		, ISS_VOID_IND                                       as                                       ISS_VOID_IND
		, ISD_VOID_IND                                       as                                       ISD_VOID_IND
		, ISDA_VOID_IND                                      as                                      ISDA_VOID_IND
		, INDMN_PLAN_CREATE_USER_LGN_NM                      as                    INDMN_PLAN_CREATE_USER_LGN_NAME
		, CLAIM_AUTH_USER_LGN_NM                             as                           CLAIM_AUTH_USER_LGN_NAME
		, INDM_SCH_FST_PRCS_DT                               as                             INDM_SCH_FST_PRCS_DATE
		, INDM_SCH_DTL_DRV_EFF_DT                            as                        INDM_SCH_DATEL_DRV_EFF_DATE
		, INDM_SCH_DTL_DRV_END_DT                            as                        INDM_SCH_DATEL_DRV_END_DATE
		, INDM_SCH_DLVR_DT                                   as                                 INDM_SCH_DLVR_DATE
		, INDM_SCH_DTL_PRCS_DT                               as                           INDM_SCH_DATEL_PRCS_DATE
		, PAY_MEDA_PREF_TYP_CD                               as                             PAY_MEDA_PREF_TYP_CODE
		, PAY_REQS_TYP_CD                                    as                                  PAY_REQS_TYP_CODE
		, PAY_REQS_STT_TYP_CD                                as                              PAY_REQS_STT_TYP_CODE
		, PAY_REQS_STS_TYP_CD                                as                              PAY_REQS_STS_TYP_CODE
		, PAY_REQS_STS_RSN_TYP_CD                            as                          PAY_REQS_STS_RSN_TYP_CODE
		, PAY_REQS_DT                                        as                                       WARRANT_DATE
		, PAY_REQS_NO                                        as                                     WARRANT_NUMBER
		, CUST_CHLD_SUPT_CASE_NO                             as                          CHILD_SUPPORT_CASE_NUMBER
		, CUST_CHLD_SUPT_WH_AMT                              as                      CHILD_SUPPORT_WITHHOLD_AMOUNT
		, INDM_PAY_DRV_TOT_AMT                               as                        INDEMNITY_PLAN_TOTAL_AMOUNT
		, INDM_PAY_DRV_SCH_TOT_AMT                           as                   INDEMNITY_SCHEDULED_TOTAL_AMOUNT
		, INDM_SCH_NO_OF_PAY                                 as                             SCHEDULE_PAYMENT_COUNT
		, INDM_SCH_DTL_DRV_DD                                as                  SCHEDULE_DETAIL_DERIVED_DAY_COUNT
		, INDM_SCH_DTL_DRV_AMT                               as                             SCHEDULE_DETAIL_AMOUNT
		, INDM_SCH_DTL_OFST_AMT                              as                      SCHEDULE_DETAIL_OFFSET_AMOUNT
		, INDM_SCH_DTL_LMP_RED_AMT                           as                           LUMPSUM_REDUCTION_AMOUNT
		, INDM_SCH_DTL_OTHR_RED_AMT                          as                             OTHER_REDUCTION_AMOUNT
		, INDM_SCH_DTL_DRV_NET_AMT                           as                         SCHEDULE_DETAIL_NET_AMOUNT
		, INDM_SCH_DTL_AMT_AMT                               as                   SCHEDULE_LINE_ITEM_DETAIL_AMOUNT
		, PP_LOSS_PRCNT                                      as                          PERMANENT_PARTIAL_PERCENT
		, SCH_AWARDS_INJURY_TYP_CODE                         as                                   INJURY_TYPE_CODE
		, SCH_AWARDS_INJURY_TYP_NAME                         as                                   INJURY_TYPE_NAME
		, INDM_PAY_DRV_WEK                                   as                  INDEMNITY_BENEFIT_PLAN_WEEK_COUNT
		, INDM_PAY_DRV_DD                                    as                   INDEMNITY_BENEFIT_PLAN_DAY_COUNT
		, DISPUTED_TRANSACTION_DAY_COUNT                     as                     DISPUTED_TRANSACTION_DAY_COUNT 
				FROM     LOGIC_IP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_IP                             as ( SELECT * from    RENAME_IP   ),

---- JOIN LAYER ----

 JOIN_IP  as  ( SELECT * 
				FROM  FILTER_IP )
 SELECT 
  UNIQUE_ID_KEY
, INDEMNITY_PLAN_ID
, INDEMNITY_SCHEDULE_PAYMENT_DETAIL_ID
, CLAIM_NUMBER
, AGRE_ID
, BENEFIT_TYPE_CODE
, BENEFIT_TYPE_DESC
, BENEFIT_CATEGORY_TYPE_CODE
, BENEFIT_CATEGORY_TYPE_DESC
, JURISDICTION_TYPE_CODE
, JURISDICTION_TYPE_DESC
, BNFT_RPT_TYP_CODE
, BENEFIT_REPORTING_TYPE_DESC
, CUST_NO
, PTCP_TYP_CODE
, CLM_PTCP_PRI_IND
, PAYEE_CHECK_GROUP_NUMBER
, INDM_PAY_EFF_DATE
, INDM_PAY_END_DATE
, INDM_FREQ_TYP_CODE
, INDM_RSN_TYP_CODE
, INDM_SCH_DTL_AMT_TYP_CODE
, INDM_SCH_DTL_STS_TYP_CODE
, INDM_SCH_DTL_FNL_PAY_IND
, INDM_SCH_AUTO_PAY_IND
, INDM_PAY_RECALC_IND
, INDM_SCH_DTL_AMT_PRI_IND
, INDM_SCH_DTL_AMT_MAILTO_IND
, INDM_SCH_DTL_AMT_RMND_IND
, OVR_PYMNT_BAL_IND
, IP_VOID_IND
, ISS_VOID_IND
, ISD_VOID_IND
, ISDA_VOID_IND
, INDMN_PLAN_CREATE_USER_LGN_NAME
, CLAIM_AUTH_USER_LGN_NAME
, INDM_SCH_FST_PRCS_DATE
, INDM_SCH_DATEL_DRV_EFF_DATE
, INDM_SCH_DATEL_DRV_END_DATE
, INDM_SCH_DLVR_DATE
, INDM_SCH_DATEL_PRCS_DATE
, PAY_MEDA_PREF_TYP_CODE
, PAY_REQS_TYP_CODE
, PAY_REQS_STT_TYP_CODE
, PAY_REQS_STS_TYP_CODE
, PAY_REQS_STS_RSN_TYP_CODE
, WARRANT_DATE
, WARRANT_NUMBER
, CHILD_SUPPORT_CASE_NUMBER
, CHILD_SUPPORT_WITHHOLD_AMOUNT
, INDEMNITY_PLAN_TOTAL_AMOUNT
, INDEMNITY_SCHEDULED_TOTAL_AMOUNT
, SCHEDULE_PAYMENT_COUNT
, SCHEDULE_DETAIL_DERIVED_DAY_COUNT
, SCHEDULE_DETAIL_AMOUNT
, SCHEDULE_DETAIL_OFFSET_AMOUNT
, LUMPSUM_REDUCTION_AMOUNT
, OTHER_REDUCTION_AMOUNT
, SCHEDULE_DETAIL_NET_AMOUNT
, SCHEDULE_LINE_ITEM_DETAIL_AMOUNT
, PERMANENT_PARTIAL_PERCENT
, INJURY_TYPE_CODE
, INJURY_TYPE_NAME
, INDEMNITY_BENEFIT_PLAN_WEEK_COUNT
, INDEMNITY_BENEFIT_PLAN_DAY_COUNT
, DISPUTED_TRANSACTION_DAY_COUNT
 FROM  JOIN_IP
  );
