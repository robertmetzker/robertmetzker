---- SRC LAYER ----
WITH
-----------------------------------------------------------------------------------------------------
-- Added filter condition here due to large volume of data in STG_ACTIVITY, STG_ACTIVITY_DETAIL Table
-----------------------------------------------------------------------------------------------------
SRC_A              as ( SELECT *     FROM     STAGING.STG_ACTIVITY 
                                            WHERE CNTX_TYP_NM IN ('CASE','CLAIM','FINANCIAL','TASK', 'BLOCK', 'CLAIM RESERVES') ),
SRC_AD             as ( SELECT *     FROM     STAGING.STG_ACTIVITY_DETAIL WHERE ACTV_ID IN (SELECT ACTV_ID FROM SRC_A)),
SRC_CL             as ( SELECT *     FROM     STAGING.STG_CLAIM ),
SRC_U              as ( SELECT *     FROM     STAGING.STG_USERS ),
SRC_CSH            as ( SELECT *     FROM     STAGING.STG_CLAIM_STATUS_HISTORY ),
SRC_CTH            as ( SELECT *     FROM     STAGING.STG_CLAIM_TYPE_HISTORY ),
SRC_ASG            as ( SELECT *     FROM     STAGING.STG_ASSIGNMENT ),
SRC_SCPH           as ( SELECT *     FROM     STAGING.STG_CLAIM_PROFILE_HISTORY ),
SRC_CAN            as ( SELECT *     FROM     STAGING.STG_CLAIM_ALIAS_NUMBER ),
SRC_CPH            as ( SELECT *     FROM     STAGING.STG_CLAIM_POLICY_HISTORY ),
SRC_PSR            as ( SELECT *     FROM     STAGING.STG_POLICY_STATUS_REASON_HISTORY ),
SRC_CH             as ( SELECT *     FROM     STAGING.STG_CLAIM_HISTORY ),
SRC_PRST           as ( SELECT *     FROM     STAGING.STG_PAYMENT_REQUEST ),
SRC_PRS            as ( SELECT *     FROM     STAGING.STG_PAYMENT_REQUEST_STATUS ),
SRC_PRSS           as ( SELECT *     FROM     STAGING.STG_PAYMENT_REQUEST ),
SRC_CFTP           as ( SELECT *     FROM     STAGING.STG_CLAIM_FINANCIAL_TRAN_PAY_REQS ),
SRC_CFTA           as ( SELECT *     FROM     STAGING.STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED ),
SRC_CFT            as ( SELECT *     FROM     STAGING.STG_CLAIM_FINANCIAL_TRANSACTION ),
SRC_CRS            as ( SELECT *     FROM     STAGING.STG_CLAIM_RESERVE ),
SRC_TSK            as ( SELECT *     FROM     STAGING.STG_TASK ),
SRC_CSES           as ( SELECT *     FROM     STAGING.STG_CASES ),
SRC_CBLK           as ( SELECT *     FROM     STAGING.STG_CUSTOMER_BLOCK ),
SRC_P              as ( SELECT *     FROM     STAGING.STG_PARTICIPATION ),
//SRC_A              as ( SELECT *     FROM     STG_ACTIVITY) ,
//SRC_AD             as ( SELECT *     FROM     STG_ACTIVITY_DETAIL) ,
//SRC_CL             as ( SELECT *     FROM     STG_CLAIM) ,
//SRC_U              as ( SELECT *     FROM     STG_USERS) ,
//SRC_CSH            as ( SELECT *     FROM     STG_CLAIM_STATUS_HISTORY) ,
//SRC_CTH            as ( SELECT *     FROM     STG_CLAIM_TYPE_HISTORY) ,
//SRC_ASG            as ( SELECT *     FROM     STG_ASSIGNMENT) ,
//SRC_FF             as ( SELECT *     FROM     STG_CLAIM_PROFILE_HISTORY) ,
//SRC_CE             as ( SELECT *     FROM     STG_CLAIM_PROFILE_HISTORY) ,
//SRC_CEW            as ( SELECT *     FROM     STG_CLAIM_PROFILE_HISTORY) ,
//SRC_CHCW           as ( SELECT *     FROM     STG_CLAIM_PROFILE_HISTORY) ,
//SRC_CAN            as ( SELECT *     FROM     STG_CLAIM_ALIAS_NUMBER) ,
//SRC_SB             as ( SELECT *     FROM     STG_CLAIM_PROFILE_HISTORY) ,
//SRC_PREM1          as ( SELECT *     FROM     STG_CLAIM_PROFILE_HISTORY) ,
//SRC_KE             as ( SELECT *     FROM     STG_CLAIM_PROFILE_HISTORY) ,
//SRC_KT             as ( SELECT *     FROM     STG_CLAIM_PROFILE_HISTORY) ,
//SRC_KRSN           as ( SELECT *     FROM     STG_CLAIM_PROFILE_HISTORY) ,
//SRC_CPH            as ( SELECT *     FROM     STG_CLAIM_POLICY_HISTORY) ,
//SRC_PSR            as ( SELECT *     FROM     STG_POLICY_STATUS_REASON_HISTORY) ,
//SRC_CH             as ( SELECT *     FROM     STG_CLAIM_HISTORY) ,
//SRC_PRST           as ( SELECT *     FROM     STG_PAYMENT_REQUEST) ,
//SRC_PRS            as ( SELECT *     FROM     STG_PAYMENT_REQUEST_STATUS) ,
//SRC_PRSS           as ( SELECT *     FROM     STG_PAYMENT_REQUEST) ,
//SRC_CFTP           as ( SELECT *     FROM     STG_CLAIM_FINANCIAL_TRAN_PAY_REQS) ,
//SRC_CFTA           as ( SELECT *     FROM     STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED) ,
//SRC_CFT            as ( SELECT *     FROM     STG_CLAIM_FINANCIAL_TRANSACTION) ,
//SRC_CRS            as ( SELECT *     FROM     STG_CLAIM_RESERVE) ,
//SRC_TSK            as ( SELECT *     FROM     STG_TASK) ,
//SRC_CSES           as ( SELECT *     FROM     STG_CASES) ,
//SRC_CBLK           as ( SELECT *     FROM     STG_CUSTOMER_BLOCK) ,
//SRC_P              as ( SELECT *     FROM     STG_PARTICIPATION) ,

---- LOGIC LAYER ----


LOGIC_A as ( SELECT 
		  ACTV_ID                                            as                                            ACTV_ID 
		, TRIM( CNTX_TYP_NM )                                as                                        CNTX_TYP_NM 
		, TRIM( SUBLOC_TYP_NM )                              as                                      SUBLOC_TYP_NM 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, TRIM( CNTX_TYP_CD )                                as                                        CNTX_TYP_CD 
		, ACTV_CNTX_ID                                       as                                       ACTV_CNTX_ID 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		FROM SRC_A
            ),

LOGIC_AD as ( SELECT 
		  ACTV_DTL_ID                                        as                                        ACTV_DTL_ID 
		, TRIM( ACTV_NM_TYP_NM )                             as                                     ACTV_NM_TYP_NM 
		, TRIM( ACTV_ACTN_TYP_NM )                           as                                   ACTV_ACTN_TYP_NM 
		, TRIM( ACTV_DTL_DESC )                              as                                      ACTV_DTL_DESC 
		, TRIM( ACTV_DTL_COL_NM )                            as                                    ACTV_DTL_COL_NM 
		, ACTV_ID                                            as                                            ACTV_ID 
		, ACTV_DTL_SUB_CNTX_ID                               as                               ACTV_DTL_SUB_CNTX_ID 
		FROM SRC_AD
            ),

LOGIC_CL as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( CLM_TYP_CD )                                 as                                         CLM_TYP_CD 
		, TRIM( NOI_CTG_TYP_NM )                             as                                     NOI_CTG_TYP_NM 
		, TRIM( NOI_TYP_NM )                                 as                                         NOI_TYP_NM 
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE 
		, TRIM( OCCR_SRC_TYP_NM )                            as                                    OCCR_SRC_TYP_NM 
		, TRIM( OCCR_MEDA_TYP_NM )                           as                                   OCCR_MEDA_TYP_NM 
		, TRIM( CLM_CTRPH_INJR_IND )                         as                                 CLM_CTRPH_INJR_IND 
		, TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND 
		, AGRE_ID                                            as                                            AGRE_ID 
		FROM SRC_CL
            ),

LOGIC_U as ( SELECT 
		  TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		, TRIM( USER_DRV_UPCS_NM )                           as                                   USER_DRV_UPCS_NM 
		, TRIM( SUPERVISOR_DRV_UPCS_NM )                     as                             SUPERVISOR_DRV_UPCS_NM 
		, USER_ID                                            as                                            USER_ID 
		FROM SRC_U
            ),

LOGIC_CSH as ( SELECT 
		  TRIM( CLM_STT_TYP_CD )                             as                                     CLM_STT_TYP_CD 
		, TRIM( CLM_STS_TYP_CD )                             as                                     CLM_STS_TYP_CD 
		, TRIM( CLM_TRANS_RSN_TYP_CD )                       as                               CLM_TRANS_RSN_TYP_CD 
        , HIST_ID                                            as                                            HIST_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, cast( HIST_END_DTM as DATE )                       as                                       HIST_END_DTM 
		, cast( HIST_EFF_DTM as DATE )                       as                                       HIST_EFF_DTM 
		FROM SRC_CSH
            ),

LOGIC_CTH as ( SELECT 
          DECODE(conditional_change_event(NVL(CLM_TYP_CD,'Z')) OVER (PARTITION BY AGRE_ID ORDER BY HIST_EFF_DTM, NVL(HIST_END_DTM,CURRENT_DATE)),0,'N','Y') as CHNG_OVR_IND
        , HIST_ID                                            as                                            HIST_ID 
        , AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_TYP_CD )                                 as                                         CLM_TYP_CD 
		, cast( HIST_END_DTM as DATE )                       as                                       HIST_END_DTM 
		, cast( HIST_EFF_DTM as DATE )                       as                                       HIST_EFF_DTM 
		FROM SRC_CTH
            ),

LOGIC_ASG as ( SELECT 
		  TRIM( FNCT_ROLE_NM )                               as                                       FNCT_ROLE_NM 
		, TRIM( ORG_UNT_NM )                                 as                                         ORG_UNT_NM 
		, TRIM( ORG_UNT_ABRV_NM )                            as                                    ORG_UNT_ABRV_NM 
		, ASGN_CNTX_ID                                       as                                       ASGN_CNTX_ID 
		, ASGN_EFF_DT                                        as                                        ASGN_EFF_DT 
		, ASGN_END_DT                                        as                                        ASGN_END_DT 
		, TRIM( APP_CNTX_TYP_CD )                            as                                    APP_CNTX_TYP_CD 
		, TRIM( FNCT_ROLE_VOID_IND )                         as                                 FNCT_ROLE_VOID_IND 
		, TRIM( ASGN_PRI_OWNR_IND )                          as                                  ASGN_PRI_OWNR_IND 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, ASGN_ID                                            as                                            ASGN_ID 
		FROM SRC_ASG
            ),

LOGIC_SCPH as ( SELECT 
		  TRIM( CLM_PRFL_ANSW_TEXT )                         as                                 CLM_PRFL_ANSW_TEXT 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
        , CASE CLM_PRFL_ANSW_TEXT WHEN 'YES' THEN 'Y'
WHEN 'NO' THEN 'N' WHEN 'UNK' THEN 'U' ELSE CLM_PRFL_ANSW_TEXT END as                            CLM_PRFL_ANSW_IND 
		, TRIM( CLM_PRFL_CTG_TYP_CD )                        as                                CLM_PRFL_CTG_TYP_CD 
        , TRIM( PRFL_SEL_VAL_TYP_NM )                        as                                PRFL_SEL_VAL_TYP_NM 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, cast( HIST_EFF_DTM as DATE )                       as                                       HIST_EFF_DTM 
		, cast( HIST_END_DTM as DATE )                       as                                       HIST_END_DTM 
		FROM SRC_SCPH
        WHERE VOID_IND = 'N' AND ((PRFL_STMT_ID =6000310 AND CLM_PRFL_CTG_TYP_CD = 'JUR') OR PRFL_STMT_ID IN (6334002,6380000,6380001,6380002,6000355,6000349,6000260,6000352))
        QUALIFY(ROW_NUMBER () OVER (PARTITION BY AGRE_ID, PRFL_STMT_ID, HIST_EFF_DTM::DATE ORDER BY HIST_EFF_DTM DESC, AUDIT_USER_CREA_DTM DESC  )) = 1 
            ),

LOGIC_CAN as ( SELECT 
		  TRIM( CLM_ALIAS_NO_NO )                            as                                    CLM_ALIAS_NO_NO 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, TRIM( CLM_ALIAS_NO_TYP_CD )                        as                                CLM_ALIAS_NO_TYP_CD 
		FROM SRC_CAN
            ),

LOGIC_CPH as ( SELECT 
		  TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, INS_PARTICIPANT                                    as                                    INS_PARTICIPANT 
		, CASE WHEN CLM_PLCY_RLTNS_EFF_DATE <> NVL(CLM_PLCY_RLTNS_END_DATE, CURRENT_DATE)
               AND CLM_PLCY_RLTNS_EFF_DATE <= LAG(CLM_PLCY_RLTNS_END_DATE) OVER(PARTITION BY AGRE_ID ORDER BY CLM_PLCY_RLTNS_EFF_DATE, NVL(CLM_PLCY_RLTNS_END_DATE, CURRENT_DATE))
               THEN  LAG(CLM_PLCY_RLTNS_END_DATE) OVER(PARTITION BY AGRE_ID ORDER BY CLM_PLCY_RLTNS_EFF_DATE, NVL(CLM_PLCY_RLTNS_END_DATE, CURRENT_DATE))+1 
               ELSE CLM_PLCY_RLTNS_EFF_DATE END              as                            CLM_PLCY_RLTNS_EFF_DATE 
		, CLM_PLCY_RLTNS_END_DATE                            as                            CLM_PLCY_RLTNS_END_DATE 
		, AGRE_ID                                            as                                            AGRE_ID 
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID 
		FROM SRC_CPH
            ),

LOGIC_PSR as ( SELECT 
		  TRIM( PLCY_TYP_CODE )                              as                                      PLCY_TYP_CODE 
		, TRIM( PLCY_STS_TYP_CD )                            as                                    PLCY_STS_TYP_CD 
		, TRIM( PLCY_STS_RSN_TYP_CD )                        as                                PLCY_STS_RSN_TYP_CD 
		, CASE WHEN PLCY_STS_TYP_CD IN ('EXP', 'ACT') THEN 'Y' 
		       WHEN  PLCY_NO IS NULL THEN NULL ELSE 'N' END  as                                  POLICY_ACTIVE_IND 
		, TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, TRIM( CRNT_PLCY_PRD_STS_RSN_IND )                  as                          CRNT_PLCY_PRD_STS_RSN_IND 
		, TRIM( MOST_RCNT_PLCY_STS_RSN_IND )                 as                         MOST_RCNT_PLCY_STS_RSN_IND 
		, AGRE_ID                                            as                                            AGRE_ID 
		FROM SRC_PSR
            ),

LOGIC_CH as ( SELECT 
		  CLM_OCCR_RPT_DATE                                  as                                  CLM_OCCR_RPT_DATE 
		, AGRE_ID                                            as                                            AGRE_ID 
		FROM SRC_CH
            ),

LOGIC_PRST as ( SELECT 
		  PAY_REQS_ID                                        as                                        PAY_REQS_ID 
		, TRIM( PAY_REQS_TYP_CD )                            as                                    PAY_REQS_TYP_CD 
		FROM SRC_PRST
            ),

LOGIC_PRS as ( SELECT 
		  PAY_REQS_STS_ID                                    as                                    PAY_REQS_STS_ID 
		, PAY_REQS_ID                                        as                                        PAY_REQS_ID 
		FROM SRC_PRS
            ),

LOGIC_PRSS as ( SELECT 
		  PAY_REQS_ID                                        as                                        PAY_REQS_ID 
		, TRIM( PAY_REQS_TYP_CD )                            as                                    PAY_REQS_TYP_CD 
		FROM SRC_PRSS
            ),

LOGIC_CFTP as ( SELECT 
		  CFT_ID                                             as                                             CFT_ID 
		, PAY_REQS_ID                                        as                                        PAY_REQS_ID 
		FROM SRC_CFTP
            ),

LOGIC_CFTA as ( SELECT 
		  CFT_ID_APLD_TO                                     as                                     CFT_ID_APLD_TO 
		, CFT_ID_APLD_FR                                     as                                     CFT_ID_APLD_FR 
		FROM SRC_CFTA
            ),

LOGIC_CFT as ( SELECT 
		  CFT_ID                                             as                                             CFT_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		FROM SRC_CFT
            ),

LOGIC_CRS as ( SELECT 
		  CLM_RSRV_ID                                        as                                        CLM_RSRV_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		FROM SRC_CRS
            ),

LOGIC_TSK as ( SELECT 
		  TASK_ID                                            as                                            TASK_ID 
		, TRIM( APP_CNTX_TYP_CD )                            as                                    APP_CNTX_TYP_CD 
		, TASK_CNTX_ID                                       as                                       TASK_CNTX_ID 
		FROM SRC_TSK
            ),

LOGIC_CSES as ( SELECT 
		  CASE_ID                                            as                                            CASE_ID 
		, TRIM( APP_CNTX_TYP_CD )                            as                                    APP_CNTX_TYP_CD 
		, CASE_CNTX_ID                                       as                                       CASE_CNTX_ID 
		FROM SRC_CSES
            ),

LOGIC_CBLK as ( SELECT 
		  CBRB_BLK_ID                                        as                                        CBRB_BLK_ID 
		, CUST_ID                                            as                                            CUST_ID 
		FROM SRC_CBLK
            ),

LOGIC_P as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		, AGRE_ID                                            as                                            AGRE_ID 
		FROM SRC_P
            )

---- RENAME LAYER ----
,

RENAME_A          as ( SELECT 
		  ACTV_ID                                            as                                            ACTV_ID
		, CNTX_TYP_NM                                        as                                        CNTX_TYP_NM
		, SUBLOC_TYP_NM                                      as                                      SUBLOC_TYP_NM
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, CNTX_TYP_CD                                        as                                        CNTX_TYP_CD
		, ACTV_CNTX_ID                                       as                                       ACTV_CNTX_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
				FROM     LOGIC_A   ), 
RENAME_AD         as ( SELECT 
		  ACTV_DTL_ID                                        as                                        ACTV_DTL_ID
		, ACTV_NM_TYP_NM                                     as                                     ACTV_NM_TYP_NM
		, ACTV_ACTN_TYP_NM                                   as                                   ACTV_ACTN_TYP_NM
		, ACTV_DTL_DESC                                      as                                      ACTV_DTL_DESC
		, ACTV_DTL_COL_NM                                    as                                    ACTV_DTL_COL_NM
		, ACTV_ID                                            as                                         AD_ACTV_ID
		, ACTV_DTL_SUB_CNTX_ID                               as                               ACTV_DTL_SUB_CNTX_ID 
				FROM     LOGIC_AD   ), 
RENAME_CL         as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, NOI_CTG_TYP_NM                                     as                                     NOI_CTG_TYP_NM
		, NOI_TYP_NM                                         as                                         NOI_TYP_NM
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, OCCR_SRC_TYP_NM                                    as                                    OCCR_SRC_TYP_NM
		, OCCR_MEDA_TYP_NM                                   as                                   OCCR_MEDA_TYP_NM
		, CLM_CTRPH_INJR_IND                                 as                                 CLM_CTRPH_INJR_IND
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, AGRE_ID                                            as                                         CL_AGRE_ID 
				FROM     LOGIC_CL   ), 
RENAME_U          as ( SELECT 
		  USER_LGN_NM                                        as                                        USER_LGN_NM
		, USER_DRV_UPCS_NM                                   as                                   USER_DRV_UPCS_NM
		, SUPERVISOR_DRV_UPCS_NM                             as                             SUPERVISOR_DRV_UPCS_NM
		, USER_ID                                            as                                            USER_ID 
				FROM     LOGIC_U   ), 
RENAME_CSH        as ( SELECT 
		  CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, HIST_ID                                            as                                        CSH_HIST_ID
        , AGRE_ID                                            as                                        CSH_AGRE_ID
		, HIST_END_DTM                                       as                                   CSH_HIST_END_DTM
		, HIST_EFF_DTM                                       as                                   CSH_HIST_EFF_DTM 
				FROM     LOGIC_CSH   ), 
RENAME_CTH        as ( SELECT 
		  CHNG_OVR_IND                                       as                                       CHNG_OVR_IND
		, HIST_ID                                            as                                        CTH_HIST_ID
		, AGRE_ID                                            as                                        CTH_AGRE_ID
		, CLM_TYP_CD                                         as                                     CTH_CLM_TYP_CD
		, HIST_END_DTM                                       as                                   CTH_HIST_END_DTM
		, HIST_EFF_DTM                                       as                                   CTH_HIST_EFF_DTM 
				FROM     LOGIC_CTH   ), 
RENAME_ASG        as ( SELECT 
		  FNCT_ROLE_NM                                       as                                       FNCT_ROLE_NM
		, ORG_UNT_NM                                         as                                         ORG_UNT_NM
		, ORG_UNT_ABRV_NM                                    as                                    ORG_UNT_ABRV_NM
		, ASGN_CNTX_ID                                       as                                       ASGN_CNTX_ID
		, ASGN_EFF_DT                                        as                                        ASGN_EFF_DT
		, ASGN_END_DT                                        as                                        ASGN_END_DT
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, FNCT_ROLE_VOID_IND                                 as                                 FNCT_ROLE_VOID_IND
		, ASGN_PRI_OWNR_IND                                  as                                  ASGN_PRI_OWNR_IND
		, AUDIT_USER_CREA_DTM                                as                            ASG_AUDIT_USER_CREA_DTM
		, ASGN_ID                                            as                                            ASGN_ID 
				FROM     LOGIC_ASG   ), 
RENAME_SCPH         as ( SELECT 
		  CLM_PRFL_ANSW_TEXT                                 as                                 CLM_PRFL_ANSW_TEXT
		, AGRE_ID                                            as                                       SCPH_AGRE_ID
		, VOID_IND                                           as                                      SCPH_VOID_IND
		, PRFL_STMT_ID                                       as                                  SCPH_PRFL_STMT_ID
        , CLM_PRFL_ANSW_IND                                  as                                  CLM_PRFL_ANSW_IND
		, CLM_PRFL_CTG_TYP_CD                                as                           SCPH_CLM_PRFL_CTG_TYP_CD
        , PRFL_SEL_VAL_TYP_NM                                as                           SCPH_PRFL_SEL_VAL_TYP_NM
		, AUDIT_USER_CREA_DTM                                as                           SCPH_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                           SCPH_AUDIT_USER_UPDT_DTM
		, HIST_EFF_DTM                                       as                                  SCPH_HIST_EFF_DTM
		, HIST_END_DTM                                       as                                  SCPH_HIST_END_DTM 
				FROM     LOGIC_SCPH  ),  
RENAME_CAN        as ( SELECT 
		  CLM_ALIAS_NO_NO                                    as                                    CLM_ALIAS_NO_NO
		, VOID_IND                                           as                                       CAN_VOID_IND
		, CLM_ALIAS_NO_TYP_CD                                as                                CLM_ALIAS_NO_TYP_CD
				FROM     LOGIC_CAN   ), 
RENAME_CPH        as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO
		, INS_PARTICIPANT                                    as                                    INS_PARTICIPANT
		, CLM_PLCY_RLTNS_EFF_DATE                            as                            CLM_PLCY_RLTNS_EFF_DATE
		, CLM_PLCY_RLTNS_END_DATE                            as                            CLM_PLCY_RLTNS_END_DATE
		, AGRE_ID                                            as                                        CPH_AGRE_ID
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID
				FROM     LOGIC_CPH   ), 
RENAME_PSR        as ( SELECT 
		  PLCY_TYP_CODE                                      as                                   POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, PLCY_NO                                            as                                        PSR_PLCY_NO
		, CRNT_PLCY_PRD_STS_RSN_IND                          as                          CRNT_PLCY_PRD_STS_RSN_IND
		, MOST_RCNT_PLCY_STS_RSN_IND                         as                         MOST_RCNT_PLCY_STS_RSN_IND
		, AGRE_ID                                            as                                        PSR_AGRE_ID
				FROM     LOGIC_PSR   ), 
RENAME_CH         as ( SELECT 
		  CLM_OCCR_RPT_DATE                                  as                            CLAIM_INITIAL_FILE_DATE
		, AGRE_ID                                            as                                         CH_AGRE_ID 
				FROM     LOGIC_CH   ),
RENAME_PRST       as ( SELECT 
		  PAY_REQS_ID                                        as                                        PAY_REQS_ID
		, PAY_REQS_TYP_CD                                    as                                    PAY_REQS_TYP_CD 
				FROM     LOGIC_PRST   ), 
RENAME_PRS        as ( SELECT 
		  PAY_REQS_STS_ID                                    as                                    PAY_REQS_STS_ID
		, PAY_REQS_ID                                        as                                    PRS_PAY_REQS_ID 
				FROM     LOGIC_PRS   ), 
RENAME_PRSS       as ( SELECT 
		  PAY_REQS_ID                                        as                                   PRSS_PAY_REQS_ID
		, PAY_REQS_TYP_CD                                    as                               PRSS_PAY_REQS_TYP_CD 
				FROM     LOGIC_PRSS   ), 
RENAME_CFTP       as ( SELECT 
		  CFT_ID                                             as                                        CFTP_CFT_ID
		, PAY_REQS_ID                                        as                                   CFTP_PAY_REQS_ID 
				FROM     LOGIC_CFTP   ), 
RENAME_CFTA       as ( SELECT 
		  CFT_ID_APLD_TO                                     as                                     CFT_ID_APLD_TO
		, CFT_ID_APLD_FR                                     as                                     CFT_ID_APLD_FR 
				FROM     LOGIC_CFTA   ), 
RENAME_CFT        as ( SELECT 
		  CFT_ID                                             as                                             CFT_ID
		, AGRE_ID                                            as                                        CFT_AGRE_ID 
				FROM     LOGIC_CFT   ), 
RENAME_CRS        as ( SELECT 
		  CLM_RSRV_ID                                        as                                        CLM_RSRV_ID
		, AGRE_ID                                            as                                        CRS_AGRE_ID 
				FROM     LOGIC_CRS   ), 
RENAME_TSK        as ( SELECT 
		  TASK_ID                                            as                                            TASK_ID
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, TASK_CNTX_ID                                       as                                       TASK_CNTX_ID 
				FROM     LOGIC_TSK   ), 
RENAME_CSES       as ( SELECT 
		  CASE_ID                                            as                                            CASE_ID
		, APP_CNTX_TYP_CD                                    as                               CSES_APP_CNTX_TYP_CD
		, CASE_CNTX_ID                                       as                                       CASE_CNTX_ID 
				FROM     LOGIC_CSES   ), 
RENAME_CBLK       as ( SELECT 
		  CBRB_BLK_ID                                        as                                        CBRB_BLK_ID
		, CUST_ID                                            as                                       CBLK_CUST_ID 
				FROM     LOGIC_CBLK   ), 
RENAME_P          as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, AGRE_ID                                            as                                          P_AGRE_ID 
				FROM     LOGIC_P   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_A                              as ( SELECT * FROM    RENAME_A  ),
FILTER_AD                             as ( SELECT * FROM    RENAME_AD   ),
FILTER_U                              as ( SELECT * FROM    RENAME_U   ),
FILTER_PRST                           as ( SELECT * FROM    RENAME_PRST   ),
FILTER_PRS                            as ( SELECT * FROM    RENAME_PRS   ),
FILTER_PRSS                           as ( SELECT * FROM    RENAME_PRSS   ),
FILTER_CFTP                           as ( SELECT * FROM    RENAME_CFTP   ),
FILTER_CFTA                           as ( SELECT * FROM    RENAME_CFTA   ),
FILTER_CFT                            as ( SELECT * FROM    RENAME_CFT   ),
FILTER_CRS                            as ( SELECT * FROM    RENAME_CRS   ),
FILTER_TSK                            as ( SELECT * FROM    RENAME_TSK 
                                            WHERE APP_CNTX_TYP_CD ='CLAIM'  ),
FILTER_CSES                           as ( SELECT * FROM    RENAME_CSES 
                                            WHERE CSES_APP_CNTX_TYP_CD ='CLAIM'  ),
FILTER_CBLK                           as ( SELECT * FROM    RENAME_CBLK   ),
FILTER_P                              as ( SELECT * FROM    RENAME_P 
                                            WHERE PTCP_TYP_CD = 'CLMT'  ),
FILTER_CL                             as ( SELECT * FROM    RENAME_CL 
                                            WHERE CLM_REL_SNPSHT_IND = 'N'  ),
FILTER_CSH                            as ( SELECT * FROM    RENAME_CSH   ),
FILTER_CTH                            as ( SELECT * FROM    RENAME_CTH   ),
FILTER_ASG                            as ( SELECT * FROM    RENAME_ASG 
                                            WHERE APP_CNTX_TYP_CD = 'CLAIM' AND FNCT_ROLE_VOID_IND = 'N' ),
FILTER_SCPH                             as ( SELECT * FROM    RENAME_SCPH  ),
FILTER_CAN                            as ( SELECT * FROM    RENAME_CAN 
                                            WHERE CAN_VOID_IND = 'N' and CLM_ALIAS_NO_TYP_CD = 'DUPEXPRDCLM'  ),
FILTER_CPH                            as ( SELECT * FROM    RENAME_CPH   ),
FILTER_PSR                            as ( SELECT * FROM    RENAME_PSR 
                                            WHERE CRNT_PLCY_PRD_STS_RSN_IND = 'Y' and MOST_RCNT_PLCY_STS_RSN_IND = 'Y'  ),
FILTER_CH                             as ( SELECT CH_AGRE_ID, min(CLAIM_INITIAL_FILE_DATE) AS CLAIM_INITIAL_FILE_DATE FROM    RENAME_CH  
                                                  GROUP BY CH_AGRE_ID    ),
             
---- JOIN LAYER ----

CBLK as ( SELECT * 
				FROM FILTER_CBLK
				LEFT JOIN FILTER_P ON  FILTER_CBLK.CBLK_CUST_ID =  FILTER_P.CUST_ID  ),
CFTA as ( SELECT * 
				FROM  FILTER_CFTA
				LEFT JOIN FILTER_CFT ON  FILTER_CFTA.CFT_ID_APLD_FR =  FILTER_CFT.CFT_ID  ),
CFTP as ( SELECT * 
				FROM  FILTER_CFTP
				LEFT JOIN CFTA ON  FILTER_CFTP.CFTP_CFT_ID = CFTA.CFT_ID_APLD_TO  ),
 
-----------------------------------------------------------------------------------------------------------------------
-- Derive the CLM_AGRE_ID based on the below context to use this column in the next join conditions
-----------------------------------------------------------------------------------------------------------------------
A_D as ( SELECT * ,   CASE WHEN CNTX_TYP_CD= 'FINANCIAL' THEN CFT_AGRE_ID
                         WHEN CNTX_TYP_CD= 'CLM_RSRV' THEN ACTV_CNTX_ID
                         WHEN CNTX_TYP_CD= 'TASK' THEN TASK_CNTX_ID
                         WHEN CNTX_TYP_CD= 'CASE' THEN CASE_CNTX_ID
                         WHEN CNTX_TYP_CD= 'BLK' THEN P_AGRE_ID
                         ELSE ACTV_CNTX_ID END AS  CLM_AGRE_ID
				FROM  FILTER_A
				INNER JOIN FILTER_AD ON  FILTER_A.ACTV_ID = FILTER_AD.AD_ACTV_ID 
                        LEFT JOIN FILTER_TSK ON  FILTER_AD.ACTV_DTL_SUB_CNTX_ID =  FILTER_TSK.TASK_ID AND FILTER_A.CNTX_TYP_CD= 'TASK' 
						LEFT JOIN FILTER_U ON  FILTER_A.AUDIT_USER_ID_CREA =  FILTER_U.USER_ID 
						LEFT JOIN FILTER_PRST ON  FILTER_A.ACTV_CNTX_ID =  FILTER_PRST.PAY_REQS_ID AND FILTER_A.CNTX_TYP_CD= 'FINANCIAL' AND FILTER_AD.ACTV_DTL_COL_NM = 'PAYMENT' 
						LEFT JOIN FILTER_PRS ON  FILTER_A.ACTV_CNTX_ID = FILTER_PRS.PAY_REQS_STS_ID AND FILTER_A.CNTX_TYP_CD= 'FINANCIAL' AND FILTER_AD.ACTV_DTL_COL_NM = 'STATUS' 
						LEFT JOIN FILTER_PRSS ON  FILTER_PRS.PRS_PAY_REQS_ID =  FILTER_PRSS.PRSS_PAY_REQS_ID
				        LEFT JOIN CFTP ON  NVL(FILTER_PRST.PAY_REQS_ID, FILTER_PRSS.PRSS_PAY_REQS_ID) = CFTP.CFTP_PAY_REQS_ID AND NVL(FILTER_PRST.PAY_REQS_TYP_CD, FILTER_PRSS.PRSS_PAY_REQS_TYP_CD) = 'INDM' 
						LEFT JOIN FILTER_CRS ON  FILTER_A.ACTV_CNTX_ID =  FILTER_CRS.CLM_RSRV_ID AND FILTER_A.CNTX_TYP_CD= 'CLM_RSRV' 
						LEFT JOIN FILTER_CSES ON  FILTER_A.ACTV_CNTX_ID =  FILTER_CSES.CASE_ID AND FILTER_A.CNTX_TYP_CD= 'CASE' 
						LEFT JOIN CBLK ON  FILTER_A.ACTV_CNTX_ID = CBLK.CBRB_BLK_ID AND FILTER_A.CNTX_TYP_CD= 'BLK'  ), 

----------------------------------------------------------------------------------------------------------------------
--  Use the derived column CLM_AGRE_ID in the join conditions and use FILTER_CL.CL_NO in below PARTITION BY CLAUSE
---------------------------------------------------------------------------------------------------------- -----------
A AS (  SELECT * 
				FROM  A_D
				INNER JOIN FILTER_CL ON A_D.CLM_AGRE_ID = FILTER_CL.CL_AGRE_ID  )

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
--  ASG Join condition has more than one Assignment records due to overlap of asgn_eff_dt, below Qualify statement picks the latest record from Assignment Table
--  Claim Status History has overlap dates so the below qualify statement bring back one record per claim
--  Claim Type History has overlap dates so the below qualify statement bring back one record per claim
--  Use the derived column CLM_AGRE_ID in the join conditions
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
,CLM AS  (
       SELECT * FROM A
          LEFT JOIN FILTER_ASG ON  A.CLM_AGRE_ID =  FILTER_ASG.ASGN_CNTX_ID AND A.AUDIT_USER_CREA_DTM::DATE BETWEEN FILTER_ASG.ASGN_EFF_DT AND NVL(FILTER_ASG.ASGN_END_DT, '12/31/2099') 
         LEFT JOIN FILTER_CSH ON  A.CLM_AGRE_ID =  FILTER_CSH.CSH_AGRE_ID AND A.AUDIT_USER_CREA_DTM::DATE BETWEEN FILTER_CSH.CSH_HIST_EFF_DTM  AND NVL(FILTER_CSH.CSH_HIST_END_DTM, '12/31/2099')
         LEFT JOIN FILTER_CTH ON  A.CLM_AGRE_ID = FILTER_CTH.CTH_AGRE_ID AND A.AUDIT_USER_CREA_DTM::DATE BETWEEN FILTER_CTH.CTH_HIST_EFF_DTM  AND NVL(FILTER_CTH.CTH_HIST_END_DTM, '12/31/2099' )
         QUALIFY (ROW_NUMBER() OVER (PARTITION BY ACTV_ID, ACTV_DTL_ID, CLM_NO ORDER BY ASGN_PRI_OWNR_IND DESC, NVL(ASGN_END_DT::DATE, '12/31/2099') DESC, ASG_AUDIT_USER_CREA_DTM DESC, ASGN_ID DESC )) = 1 
         AND (ROW_NUMBER() OVER (PARTITION BY ACTV_ID, ACTV_DTL_ID, CLM_NO ORDER BY FILTER_CSH.CSH_HIST_EFF_DTM DESC, NVL(FILTER_CSH.CSH_HIST_END_DTM, '12/31/2099') DESC, CSH_HIST_ID DESC )) = 1
         AND (ROW_NUMBER() OVER (PARTITION BY ACTV_ID, ACTV_DTL_ID, CLM_NO  ORDER BY FILTER_CTH.CTH_HIST_EFF_DTM DESC, NVL(FILTER_CTH.CTH_HIST_END_DTM, '12/31/2099') DESC, CTH_HIST_ID DESC )) = 1 )

----------------------------------------------------------------------------------------------------------------------
--  Derive the INDICATOR columns from CLAIM_PROFILE_HISTORY Table by joining to above CLM Alias
---------------------------------------------------------------------------------------------------------- -----------

, AGG_ETL AS (  SELECT  SCPH_AGRE_ID
                     , MAX(CASE WHEN SCPH_PRFL_STMT_ID = 6334002 THEN CLM_PRFL_ANSW_IND END) AS FIREFIGHTER_CANCER_IND
                     , MAX(CASE WHEN SCPH_PRFL_STMT_ID = 6380000 THEN CLM_PRFL_ANSW_IND END) AS COVID_EXPOSURE_IND
                     , MAX(CASE WHEN SCPH_PRFL_STMT_ID = 6380001 THEN CLM_PRFL_ANSW_IND END) AS COVID_EMERGENCY_WORKER_IND
                     , MAX(CASE WHEN SCPH_PRFL_STMT_ID = 6380002 THEN CLM_PRFL_ANSW_IND END) AS COVID_HEALTH_CARE_WORKER_IND
                     , MAX(CASE WHEN SCPH_PRFL_STMT_ID = 6000355 THEN CLM_PRFL_ANSW_IND END) AS SB223_IND
                     , MAX(CASE WHEN SCPH_PRFL_STMT_ID = 6000310 AND SCPH_CLM_PRFL_CTG_TYP_CD = 'JUR' THEN CLM_PRFL_ANSW_IND END) AS EMPLOYER_PREMISES_IND
                     , MAX(CASE WHEN SCPH_PRFL_STMT_ID = 6000349 AND CLM_PRFL_ANSW_TEXT = 'UNK' THEN 'UNKNOWN' 
                                WHEN SCPH_PRFL_STMT_ID = 6000349 THEN CLM_PRFL_ANSW_TEXT ELSE NULL END) AS K_PROGRAM_ENROLLMENT_DESC
                     , MAX(CASE WHEN SCPH_PRFL_STMT_ID = 6000260 THEN CLM_PRFL_ANSW_TEXT END) AS K_PROGRAM_TYPE_DESC
                     , MAX(CASE WHEN SCPH_PRFL_STMT_ID = 6000352 THEN SCPH_PRFL_SEL_VAL_TYP_NM END) AS K_PROGRAM_REASON_DESC
          FROM  CLM CLM
						INNER JOIN FILTER_SCPH ON  CLM.CLM_AGRE_ID =  FILTER_SCPH.SCPH_AGRE_ID AND CLM.AUDIT_USER_CREA_DTM::DATE BETWEEN FILTER_SCPH.SCPH_HIST_EFF_DTM  AND NVL(FILTER_SCPH.SCPH_HIST_END_DTM, '12/31/2099')
                        GROUP BY SCPH_AGRE_ID
         )

--- ETL_LAYER-------

, ETL AS ( SELECT  ACTV_ID, ACTV_DTL_ID, CLM_AGRE_ID, CLM_NO, CNTX_TYP_NM, SUBLOC_TYP_NM, ACTV_NM_TYP_NM, ACTV_ACTN_TYP_NM, ACTV_DTL_DESC
        , ACTV_DTL_COL_NM, AUDIT_USER_CREA_DTM, USER_LGN_NM, USER_DRV_UPCS_NM, SUPERVISOR_DRV_UPCS_NM, CLM_TYP_CD, CLM_STT_TYP_CD, CLM_STS_TYP_CD
        , CLM_TRANS_RSN_TYP_CD, CHNG_OVR_IND, FNCT_ROLE_NM, NOI_CTG_TYP_NM, NOI_TYP_NM, CLM_OCCR_DATE, OCCR_SRC_TYP_NM, OCCR_MEDA_TYP_NM 
        , FIREFIGHTER_CANCER_IND, COVID_EXPOSURE_IND, COVID_EMERGENCY_WORKER_IND, COVID_HEALTH_CARE_WORKER_IND,nvl2(CLM_ALIAS_NO_NO, 'Y', 'N') AS COMBINED_CLAIM_IND, SB223_IND, EMPLOYER_PREMISES_IND
        , K_PROGRAM_ENROLLMENT_DESC,  K_PROGRAM_TYPE_DESC, K_PROGRAM_REASON_DESC, CLM_CTRPH_INJR_IND, PLCY_NO, BUSN_SEQ_NO, INS_PARTICIPANT, POLICY_TYPE_CODE, PLCY_STS_TYP_CD, PLCY_STS_RSN_TYP_CD
        , POLICY_ACTIVE_IND, ORG_UNT_NM, ORG_UNT_ABRV_NM, CLAIM_INITIAL_FILE_DATE 
          FROM CLM CLM 
          LEFT JOIN AGG_ETL ON CLM.CLM_AGRE_ID =  AGG_ETL.SCPH_AGRE_ID
          LEFT JOIN FILTER_CH ON  CLM.CLM_AGRE_ID =  FILTER_CH.CH_AGRE_ID 
          LEFT JOIN FILTER_CPH ON  CLM.CLM_AGRE_ID =  FILTER_CPH.CPH_AGRE_ID AND CLM.AUDIT_USER_CREA_DTM::DATE BETWEEN FILTER_CPH.CLM_PLCY_RLTNS_EFF_DATE  AND  NVL(FILTER_CPH.CLM_PLCY_RLTNS_END_DATE, '12/31/2099') 
          LEFT JOIN FILTER_PSR ON  FILTER_CPH.PLCY_AGRE_ID =  FILTER_PSR.PSR_AGRE_ID 
          LEFT JOIN FILTER_CAN ON  CLM.CLM_NO =  FILTER_CAN.CLM_ALIAS_NO_NO )


select distinct
  md5(cast(
    
    coalesce(cast(ACTV_ID as 
    varchar
), '') || '-' || coalesce(cast(ACTV_DTL_ID as 
    varchar
), '') || '-' || coalesce(cast(CLM_NO as 
    varchar
), '')

 as 
    varchar
)) AS  UNIQUE_ID_KEY
        , ACTV_ID
        , ACTV_DTL_ID
        , CLM_AGRE_ID
        , CLM_NO
        , CNTX_TYP_NM
        , SUBLOC_TYP_NM
        , ACTV_NM_TYP_NM
        , ACTV_ACTN_TYP_NM
        , ACTV_DTL_DESC
        , ACTV_DTL_COL_NM
        , AUDIT_USER_CREA_DTM
        , USER_LGN_NM
        , USER_DRV_UPCS_NM
        , SUPERVISOR_DRV_UPCS_NM
        , CLM_TYP_CD
        , CLM_STT_TYP_CD
        , CLM_STS_TYP_CD
        , CLM_TRANS_RSN_TYP_CD
        , CHNG_OVR_IND
        , FNCT_ROLE_NM
		, NOI_CTG_TYP_NM
		, NOI_TYP_NM
		, CLM_OCCR_DATE
		, OCCR_SRC_TYP_NM
		, OCCR_MEDA_TYP_NM
		, FIREFIGHTER_CANCER_IND
		, COVID_EXPOSURE_IND
		, COVID_EMERGENCY_WORKER_IND
		, COVID_HEALTH_CARE_WORKER_IND
		, COMBINED_CLAIM_IND
		, SB223_IND
		, EMPLOYER_PREMISES_IND
		, K_PROGRAM_ENROLLMENT_DESC
		, K_PROGRAM_TYPE_DESC
		, K_PROGRAM_REASON_DESC
		, CLM_CTRPH_INJR_IND
		, PLCY_NO
		, BUSN_SEQ_NO
		, INS_PARTICIPANT
		, POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND
		, ORG_UNT_NM
		, ORG_UNT_ABRV_NM
		, CLAIM_INITIAL_FILE_DATE 
 from ETL