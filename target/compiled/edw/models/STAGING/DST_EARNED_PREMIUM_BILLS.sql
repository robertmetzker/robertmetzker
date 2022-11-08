---- SRC LAYER ----
WITH
SRC_PFT            as ( SELECT *     FROM     STAGING.STG_POLICY_FINANCIAL_TRANSACTION ),
SRC_P              as ( SELECT *     FROM     STAGING.STG_POLICY ),
SRC_PP             as ( SELECT *     FROM     STAGING.STG_POLICY_PERIOD ),
SRC_PT             as ( SELECT *     FROM     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_PSRH           as ( SELECT *     FROM     STAGING.STG_POLICY_STATUS_REASON_HISTORY ),
SRC_PAY            as ( SELECT *     FROM     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_FREQ           as ( SELECT *     FROM     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_AUDT           as ( SELECT *     FROM     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_ELT            as ( SELECT *     FROM     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_BSD            as ( SELECT *     FROM     STAGING.STG_BILLING_SCHEDULE_DETAIL ),
SRC_BSDA           as ( SELECT *     FROM     STAGING.STG_BILLING_SCHEDULE_DETAIL_AMOUNT ),
SRC_AG             as ( SELECT *     FROM     STAGING.STG_CERTIFY_AG_FNCL_TRAN_DTL ),
SRC_PROF           as ( SELECT *     FROM     STAGING.STG_POLICY_PROFILE ),
SRC_U              as ( SELECT *     FROM     STAGING.STG_USERS ),
SRC_X              as ( SELECT *     FROM     STAGING.STG_BSDA_PLCY_FNCL_TRAN_XREF ),
SRC_PAUD           as ( SELECT *     FROM     STAGING.STG_POLICY_AUDIT ),
SRC_RPT            as ( SELECT *     FROM     STAGING.STG_PAYROLL_REPORT ),
SRC_PREM1          as ( SELECT *     FROM     STAGING.STG_PREMIUM_PERIOD ),
SRC_WCP1           as ( SELECT *     FROM     STAGING.STG_WC_COVERAGE_PREMIUM ),
SRC_PREM2          as ( SELECT *     FROM     STAGING.STG_PREMIUM_PERIOD ),
SRC_WCP2           as ( SELECT *     FROM     STAGING.STG_WC_COVERAGE_PREMIUM ),
SRC_PREM3          as ( SELECT *     FROM     STAGING.STG_PREMIUM_PERIOD ),
SRC_WCP3           as ( SELECT *     FROM     STAGING.STG_WC_COVERAGE_PREMIUM ),
//SRC_PFT            as ( SELECT *     FROM     STG_POLICY_FINANCIAL_TRANSACTION) ,
//SRC_P              as ( SELECT *     FROM     STG_POLICY) ,
//SRC_PP             as ( SELECT *     FROM     STG_POLICY_PERIOD) ,
//SRC_PT             as ( SELECT *     FROM     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_PSRH           as ( SELECT *     FROM     STG_POLICY_STATUS_REASON_HISTORY) ,
//SRC_PAY            as ( SELECT *     FROM     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_FREQ           as ( SELECT *     FROM     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_AUDT           as ( SELECT *     FROM     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_ELT            as ( SELECT *     FROM     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_BSD            as ( SELECT *     FROM     STG_BILLING_SCHEDULE_DETAIL) ,
//SRC_BSDA           as ( SELECT *     FROM     STG_BILLING_SCHEDULE_DETAIL_AMOUNT) ,
//SRC_AG             as ( SELECT *     FROM     STG_CERTIFY_AG_FNCL_TRAN_DTL) ,
//SRC_PROF           as ( SELECT *     FROM     STG_POLICY_PROFILE) ,
//SRC_U              as ( SELECT *     FROM     STG_USERS) ,
//SRC_X              as ( SELECT *     FROM     STG_BSDA_PLCY_FNCL_TRAN_XREF) ,
//SRC_PAUD           as ( SELECT *     FROM     STG_POLICY_AUDIT) ,
//SRC_RPT            as ( SELECT *     FROM     STG_PAYROLL_REPORT) ,
//SRC_PREM1          as ( SELECT *     FROM     STG_PREMIUM_PERIOD) ,
//SRC_WCP1           as ( SELECT *     FROM     STG_WC_COVERAGE_PREMIUM) ,
//SRC_PREM2          as ( SELECT *     FROM     STG_PREMIUM_PERIOD) ,
//SRC_WCP2           as ( SELECT *     FROM     STG_WC_COVERAGE_PREMIUM) ,
//SRC_PREM3          as ( SELECT *     FROM     STG_PREMIUM_PERIOD) ,
//SRC_WCP3           as ( SELECT *     FROM     STG_WC_COVERAGE_PREMIUM) ,


---- LOGIC LAYER ----

LOGIC_PFT as ( SELECT 
                               TRIM( PLCY_NO )                                    as                                            PLCY_NO 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
                             , PFT_ID                                             as                                             PFT_ID 
                             , PFT_DT                                             as                                             PFT_DT 
                             , PFT_DUE_DT                                         as                                         PFT_DUE_DT 
                             , PFT_AMT                                            as                                            PFT_AMT 
                             , PFT_DRV_BAL_AMT                                    as                                    PFT_DRV_BAL_AMT 
                             , FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID 
                             , TRIM( FNCL_TRAN_TYP_CD )                           as                                   FNCL_TRAN_TYP_CD 
                             , TRIM( FNCL_TRAN_TYP_NM )                           as                                   FNCL_TRAN_TYP_NM 
                             , TRIM( FNCL_TRAN_SUB_TYP_CD )                       as                               FNCL_TRAN_SUB_TYP_CD 
                             , TRIM( FNCL_TRAN_SUB_TYP_NM )                       as                               FNCL_TRAN_SUB_TYP_NM 
                             , INVC_ID                                            as                                            INVC_ID 
                             , CUST_ID                                            as                                            CUST_ID 
                             , TRIM( PFT_CMT )                                    as                                            PFT_CMT 
                             , AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
                             , AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
                             , AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
                             , PLCY_PRD_AUDT_DTL_ID                               as                               PLCY_PRD_AUDT_DTL_ID 
                             , PYRL_RPT_ID                                        as                                        PYRL_RPT_ID
                             , AGRE_ID                                            as                                            AGRE_ID     
                             FROM SRC_PFT
             ),

LOGIC_P as ( SELECT 
                               CUST_ID_ACCT_HLDR                                  as                                  CUST_ID_ACCT_HLDR 
                             , TRIM( CUST_NO )                                    as                                            CUST_NO
                             , AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
                             , AGRE_ID                                            as                                            AGRE_ID    
                             FROM SRC_P
            ),

LOGIC_PP as ( SELECT 
                               PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT 
                             , PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
                             , NEW_POLICY_IND                                     AS                                     NEW_POLICY_IND
                             , TRIM( VOID_IND )                                   as                                           VOID_IND
                             , AGRE_ID                                            as                                            AGRE_ID
                             FROM SRC_PP
            ),

LOGIC_PT as ( SELECT 
                               TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD 
                             , TRIM( CTL_ELEM_SUB_TYP_NM )                        as                                CTL_ELEM_SUB_TYP_NM 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
                             , TRIM( VOID_IND )                                   as                                           VOID_IND
                             , TRIM(CTL_ELEM_TYP_CD)                                    as                              CTL_ELEM_TYP_CD
                             FROM SRC_PT
            ),

LOGIC_PSRH as ( SELECT 
                               TRIM( PLCY_STS_TYP_CD )                            as                                    PLCY_STS_TYP_CD 
                             , TRIM( PLCY_STS_TYP_NM )                            as                                    PLCY_STS_TYP_NM 
                             , TRIM( PLCY_STS_RSN_TYP_CD )                        as                                PLCY_STS_RSN_TYP_CD 
                             , TRIM( PLCY_STS_RSN_TYP_NM )                        as                                PLCY_STS_RSN_TYP_NM 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
                             , TRIM( CRNT_PLCY_PRD_STS_RSN_IND )                  as                          CRNT_PLCY_PRD_STS_RSN_IND 
                             FROM SRC_PSRH
            ),

LOGIC_PAY as ( SELECT 
                               TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD 
                             , TRIM( CTL_ELEM_SUB_TYP_NM )                        as                                CTL_ELEM_SUB_TYP_NM 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
                             , TRIM( VOID_IND )                                   as                                           VOID_IND
                             , CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD
                             FROM SRC_PAY
            ),

LOGIC_FREQ as ( SELECT 
                               TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD 
                             , TRIM( CTL_ELEM_SUB_TYP_NM )                        as                                CTL_ELEM_SUB_TYP_NM 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
                             , TRIM( VOID_IND )                                   as                                           VOID_IND
                             , CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD
                             FROM SRC_FREQ
            ),

LOGIC_AUDT as ( SELECT 
                               TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD 
                             , TRIM( CTL_ELEM_SUB_TYP_NM )                        as                                CTL_ELEM_SUB_TYP_NM 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
                             , TRIM( VOID_IND )                                   as                                           VOID_IND
                             , CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD
                             FROM SRC_AUDT
            ),

LOGIC_ELT as ( SELECT 
                               TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD 
                             , TRIM( CTL_ELEM_SUB_TYP_NM )                        as                                CTL_ELEM_SUB_TYP_NM 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
                             , TRIM( VOID_IND )                                   as                                           VOID_IND
                             , CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD
                             FROM SRC_ELT
            ),

LOGIC_BSD as ( SELECT 
                               MIN( BILL_SCH_DTL_NO )                            as                                    BILL_SCH_DTL_NO 
                             , BILL_SCH_DTL_ID                                    as                                    BILL_SCH_DTL_ID 
                             FROM SRC_BSD
            GROUP BY BILL_SCH_DTL_ID ),

LOGIC_BSDA as ( SELECT 
                               MIN( BILL_SCH_DTL_AMT_NO )                        as                                BILL_SCH_DTL_AMT_NO 
                             , BILL_SCH_DTL_AMT_ID                                as                                BILL_SCH_DTL_AMT_ID 
                             , BILL_SCH_DTL_ID                                    as                                    BILL_SCH_DTL_ID 
                             FROM SRC_BSDA
           GROUP BY BILL_SCH_DTL_ID,BILL_SCH_DTL_AMT_ID ),

LOGIC_AG as ( SELECT 
                               CERT_AG_FNCL_TRAN_DTL_ADD_DT                      as                       CERT_AG_FNCL_TRAN_DTL_ADD_DT 
                             , PFT_ID                                             as                                             PFT_ID 
                             FROM SRC_AG
                             QUALIFY(ROW_NUMBER()OVER(PARTITION BY PFT_ID ORDER BY CERT_AG_FNCL_TRAN_DTL_ADD_DT desc))=1 
            ),

LOGIC_PROF as ( SELECT 
                              CASE WHEN PLCY_PRFL_ANSW_TEXT = 'YES' THEN 'Y' ELSE 'N' END as                     POLICY_EMPLOYER_PAID_PROGRAM_IND                                               
                             , PLCY_PRD_ID                                        as                                       PLCY_PRD_ID 
                             , PRFL_STMT_ID                                       as                                       PRFL_STMT_ID 
                             , TRIM( PLCY_PRFL_VOID_IND )                         as                                 PLCY_PRFL_VOID_IND 
                             FROM SRC_PROF
            ),

LOGIC_U as ( SELECT 
                               TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
                             , TRIM( USER_DRV_UPCS_NM )                           as                                   USER_DRV_UPCS_NM 
                             , USER_ID                                            as                                            USER_ID 
                             FROM SRC_U
            ),

LOGIC_X as ( SELECT 
                               PFT_ID                                             as                                             PFT_ID 
                             , BILL_SCH_DTL_AMT_ID                                as                                BILL_SCH_DTL_AMT_ID 
                             FROM SRC_X
            ),

LOGIC_PAUD as ( SELECT 
                               PLCY_PRD_AUDT_DTL_ID                               as                               PLCY_PRD_AUDT_DTL_ID 
                             , TRIM( PLCY_AUDT_TYP_CD )                           as                                   PLCY_AUDT_TYP_CD 
                             , PLCY_PRD_AUDT_DTL_EFF_DT                           as                           PLCY_PRD_AUDT_DTL_EFF_DT 
                             , TRIM( VOID_IND )                                   as                                           VOID_IND 
                             FROM SRC_PAUD
            ),

LOGIC_RPT as ( SELECT 
                               PYRL_RPT_ID                                        as                                        PYRL_RPT_ID 
                             , TRIM( PYRL_RPT_EST_RPT_IND )                       as                               PYRL_RPT_EST_RPT_IND 
                             , TRIM( VOID_IND )                                   as                                           VOID_IND 
                             FROM SRC_RPT
            ),

LOGIC_PREM1 as ( SELECT 
                               PREM_PRD_ID                                        as                                        PREM_PRD_ID 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
                             , TRIM( PREM_TYP_CD )                                as                                        PREM_TYP_CD 
                             , TRIM( VOID_IND )                                   as                                           VOID_IND 
                             FROM SRC_PREM1
            ),

LOGIC_WCP1 as ( SELECT 
                               PREM_PRD_ID                                        as                                         PREM_PRD_ID
                             , WC_COV_PREM_BS_VAL                                 as                                  WC_COV_PREM_BS_VAL   
                             , TRIM( WC_COV_PREM_VOID_IND )                       as                                WC_COV_PREM_VOID_IND 
                             FROM SRC_WCP1
            ),

LOGIC_PREM2 as ( SELECT 
                               PREM_PRD_ID                                        as                                        PREM_PRD_ID 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
                             , TRIM( PREM_TYP_CD )                                as                                        PREM_TYP_CD 
                             , TRIM( VOID_IND )                                   as                                           VOID_IND 
                             FROM SRC_PREM2
            ),

LOGIC_WCP2 as ( SELECT 
                               PREM_PRD_ID                                        as                                        PREM_PRD_ID
                             , WC_COV_PREM_BS_VAL                                 as                                  WC_COV_PREM_BS_VAL 
                             , TRIM( WC_COV_PREM_VOID_IND )                       as                               WC_COV_PREM_VOID_IND 
                             FROM SRC_WCP2
            ),

LOGIC_PREM3 as ( SELECT 
                               PREM_PRD_ID                                        as                                        PREM_PRD_ID 
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
                             , TRIM( PREM_TYP_CD )                                as                                        PREM_TYP_CD 
                             , TRIM( VOID_IND )                                   as                                           VOID_IND 
                             FROM SRC_PREM3
            ),

LOGIC_WCP3 as ( SELECT 
                               PREM_PRD_ID                                        as                                        PREM_PRD_ID
                             , WC_COV_PREM_BS_VAL                                 as                                  WC_COV_PREM_BS_VAL 
                             , TRIM( WC_COV_PREM_VOID_IND )                       as                                WC_COV_PREM_VOID_IND 
                             FROM SRC_WCP3
            ),

---- RENAME LAYER ----


RENAME_PFT        as ( SELECT 
                               PLCY_NO                                            as                                            PLCY_NO
                             , PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
                             , PFT_ID                                             as                                             PFT_ID
                             , PFT_DT                                             as                                             PFT_DT
                             , PFT_DUE_DT                                         as                                         PFT_DUE_DT
                             , PFT_AMT                                            as                                            PFT_AMT
                             , PFT_DRV_BAL_AMT                                    as                                    PFT_DRV_BAL_AMT
                             , FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID
                             , FNCL_TRAN_TYP_CD                                   as                                   FNCL_TRAN_TYP_CD
                             , FNCL_TRAN_TYP_NM                                   as                                   FNCL_TRAN_TYP_NM
                             , FNCL_TRAN_SUB_TYP_CD                               as                               FNCL_TRAN_SUB_TYP_CD
                             , FNCL_TRAN_SUB_TYP_NM                               as                               FNCL_TRAN_SUB_TYP_NM
                             , INVC_ID                                            as                                            INVC_ID
                             , CUST_ID                                            as                                            CUST_ID
                             , PFT_CMT                                            as                                            PFT_CMT
                             , AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
                             , AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
                             , AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
                             , PLCY_PRD_AUDT_DTL_ID                               as                               PLCY_PRD_AUDT_DTL_ID
                             , PYRL_RPT_ID                                        as                                        PYRL_RPT_ID
                             , AGRE_ID                                            as                                            AGRE_ID              
                                                          FROM     LOGIC_PFT   ), 
RENAME_P          as ( SELECT 
                               CUST_ID_ACCT_HLDR                                  as                               EMPLOYER_CUSTOMER_ID
                             , CUST_NO                                            as                           EMPLOYER_CUSTOMER_NUMBER
                             , AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
                             , AGRE_ID                                            as                                          P_AGRE_ID              
                                                          FROM     LOGIC_P   ), 
RENAME_PP         as ( SELECT 
                               PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT
                             , PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT
                             , PLCY_PRD_ID                                        as                                     PP_PLCY_PRD_ID
                             , NEW_POLICY_IND                                     AS                                     NEW_POLICY_IND
                             , VOID_IND                                           as                                           VOID_IND
                             , AGRE_ID                                            as                                         PP_AGRE_ID
                                                           FROM     LOGIC_PP   ), 
RENAME_PT         as ( SELECT 
                               CTL_ELEM_SUB_TYP_CD                                as                                   POLICY_TYPE_CODE
                             , CTL_ELEM_SUB_TYP_NM                                as                                   POLICY_TYPE_DESC
                             , PLCY_PRD_ID                                        as                                     PT_PLCY_PRD_ID
                             , VOID_IND                                           as                                        PT_VOID_IND
                             , CTL_ELEM_TYP_CD                                    as                                 PT_CTL_ELEM_TYP_CD             
                                                          FROM     LOGIC_PT   ), 
RENAME_PSRH       as ( SELECT 
                               PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
                             , PLCY_STS_TYP_NM                                    as                                    PLCY_STS_TYP_NM
                             , PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
                             , PLCY_STS_RSN_TYP_NM                                as                                PLCY_STS_RSN_TYP_NM
                             , PLCY_PRD_ID                                        as                                   PSRH_PLCY_PRD_ID
                             , CRNT_PLCY_PRD_STS_RSN_IND                          as                          CRNT_PLCY_PRD_STS_RSN_IND 
                                                          FROM     LOGIC_PSRH   ), 
RENAME_PAY        as ( SELECT 
                               CTL_ELEM_SUB_TYP_CD                                as                             PAYMENT_PLAN_TYPE_CODE
                             , CTL_ELEM_SUB_TYP_NM                                as                             PAYMENT_PLAN_TYPE_DESC
                             , PLCY_PRD_ID                                        as                                    PAY_PLCY_PRD_ID
                             , VOID_IND                                           as                                       PAY_VOID_IND
                             , CTL_ELEM_TYP_CD                                    as                                PAY_CTL_ELEM_TYP_CD
                                                          FROM     LOGIC_PAY   ), 
RENAME_FREQ       as ( SELECT 
                               CTL_ELEM_SUB_TYP_CD                                as                      REPORTING_FREQUENCY_TYPE_CODE
                             , CTL_ELEM_SUB_TYP_NM                                as                      REPORTING_FREQUENCY_TYPE_DESC
                             , PLCY_PRD_ID                                        as                                   FREQ_PLCY_PRD_ID
                             , VOID_IND                                           as                                      FREQ_VOID_IND
                             , CTL_ELEM_TYP_CD                                    as                               FREQ_CTL_ELEM_TYP_CD
                                                          FROM     LOGIC_FREQ   ), 
RENAME_AUDT       as ( SELECT 
                               CTL_ELEM_SUB_TYP_CD                                as                                    AUDIT_TYPE_CODE
                             , CTL_ELEM_SUB_TYP_NM                                as                                    AUDIT_TYPE_DESC
                             , PLCY_PRD_ID                                        as                                   AUDT_PLCY_PRD_ID
                             , VOID_IND                                           as                                      AUDT_VOID_IND
                             , CTL_ELEM_TYP_CD                                    as                                    AUDT_CTL_ELEM_TYP_CD
                                                          FROM     LOGIC_AUDT   ), 
RENAME_ELT        as ( SELECT 
                               CTL_ELEM_SUB_TYP_CD                                as                         EMPLOYEE_LEASING_TYPE_CODE
                             , CTL_ELEM_SUB_TYP_NM                                as                         EMPLOYEE_LEASING_TYPE_DESC
                             , PLCY_PRD_ID                                        as                                    ELT_PLCY_PRD_ID
                             , VOID_IND                                           as                                       ELT_VOID_IND
                             , CTL_ELEM_TYP_CD                                    as                                ELT_CTL_ELEM_TYP_CD
                                                          FROM     LOGIC_ELT   ), 
RENAME_BSD        as ( SELECT 
                               BILL_SCH_DTL_NO                                    as                                            BILL_NO
                             , BILL_SCH_DTL_ID                                    as                                BSD_BILL_SCH_DTL_ID 
                                                          FROM     LOGIC_BSD   ), 
RENAME_BSDA       as ( SELECT 
                               BILL_SCH_DTL_AMT_NO                                as                                            LINE_NO
                             , BILL_SCH_DTL_AMT_ID                                as                           BSDA_BILL_SCH_DTL_AMT_ID
                             , BILL_SCH_DTL_ID                                    as                               BSDA_BILL_SCH_DTL_ID 
                                                          FROM     LOGIC_BSDA   ), 
RENAME_AG         as ( SELECT 
                               CERT_AG_FNCL_TRAN_DTL_ADD_DT                       as                                  CERTIFIED_AG_DATE
                             , PFT_ID                                             as                                          AG_PFT_ID 
                                                          FROM     LOGIC_AG   ), 
RENAME_PROF       as ( SELECT 
                               POLICY_EMPLOYER_PAID_PROGRAM_IND                             as                             POLICY_EMPLOYER_PAID_PROGRAM_IND
                             , PLCY_PRD_ID                                        as                                   PROF_PLCY_PRD_ID
                             , PRFL_STMT_ID                                       as                                       PRFL_STMT_ID
                             , PLCY_PRFL_VOID_IND                                 as                                 PLCY_PRFL_VOID_IND 
                                                          FROM     LOGIC_PROF   ), 
RENAME_U          as ( SELECT 
                               USER_LGN_NM                                        as                             CREATE_USER_LOGIN_NAME
                             , USER_DRV_UPCS_NM                                   as                                   CREATE_USER_NAME
                             , USER_ID                                            as                                            USER_ID 
                                                          FROM     LOGIC_U   ), 
RENAME_X          as ( SELECT 
                               PFT_ID                                             as                                           X_PFT_ID
                             , BILL_SCH_DTL_AMT_ID                                as                              X_BILL_SCH_DTL_AMT_ID 
                                                          FROM     LOGIC_X   ), 
RENAME_PAUD       as ( SELECT 
                               PLCY_PRD_AUDT_DTL_ID                               as                          PAUD_PLCY_PRD_AUDT_DTL_ID
                             , PLCY_AUDT_TYP_CD                                   as                                   PLCY_AUDT_TYP_CD
                             , PLCY_PRD_AUDT_DTL_EFF_DT                           as                           PLCY_PRD_AUDT_DTL_EFF_DT
                             , VOID_IND                                           as                                      PAUD_VOID_IND 
                                                          FROM     LOGIC_PAUD   ), 
RENAME_RPT        as ( SELECT 
                               PYRL_RPT_ID                                        as                                    RPT_PYRL_RPT_ID
                             , PYRL_RPT_EST_RPT_IND                               as                               PYRL_RPT_EST_RPT_IND
                             , VOID_IND                                           as                                       RPT_VOID_IND 
                                                          FROM     LOGIC_RPT   ), 
RENAME_PREM1      as ( SELECT 
                               PREM_PRD_ID                                        as                                  PREM1_PREM_PRD_ID
                             , PLCY_PRD_ID                                        as                                  PREM1_PLCY_PRD_ID
                             , PREM_TYP_CD                                        as                                  PREM1_PREM_TYP_CD
                             , VOID_IND                                           as                                     PREM1_VOID_IND 
                                                          FROM     LOGIC_PREM1   ), 
RENAME_WCP1       as ( SELECT 
                               PREM_PRD_ID                                        as                                   WCP1_PREM_PRD_ID
                             , WC_COV_PREM_BS_VAL                                 as                                  WC_COV_PREM_BS_VAL
                             , WC_COV_PREM_VOID_IND                               as                          WCP1_WC_COV_PREM_VOID_IND 
                                                          FROM     LOGIC_WCP1   ), 
RENAME_PREM2      as ( SELECT 
                               PREM_PRD_ID                                        as                                  PREM2_PREM_PRD_ID
                             , PLCY_PRD_ID                                        as                                  PREM2_PLCY_PRD_ID
                             , PREM_TYP_CD                                        as                                  PREM2_PREM_TYP_CD
                             , VOID_IND                                           as                                     PREM2_VOID_IND 
                                                          FROM     LOGIC_PREM2   ), 
RENAME_WCP2       as ( SELECT 
                               PREM_PRD_ID                                        as                                   WCP2_PREM_PRD_ID
                             , WC_COV_PREM_BS_VAL                                 as                                  WC_COV_PREM_BS_VAL
                             , WC_COV_PREM_VOID_IND                               as                          WCP2_WC_COV_PREM_VOID_IND 
                                                          FROM     LOGIC_WCP2   ), 
RENAME_PREM3      as ( SELECT 
                               PREM_PRD_ID                                        as                                  PREM3_PREM_PRD_ID
                             , PLCY_PRD_ID                                        as                                  PREM3_PLCY_PRD_ID
                             , PREM_TYP_CD                                        as                                  PREM3_PREM_TYP_CD
                             , VOID_IND                                           as                                     PREM3_VOID_IND 
                                                          FROM     LOGIC_PREM3   ), 
RENAME_WCP3       as ( SELECT 
                               PREM_PRD_ID                                        as                                   WCP3_PREM_PRD_ID
                             , WC_COV_PREM_BS_VAL                                 as                                  WC_COV_PREM_BS_VAL
                             , WC_COV_PREM_VOID_IND                               as                          WCP3_WC_COV_PREM_VOID_IND 
                                                          FROM     LOGIC_WCP3   )


---- FILTER LAYER (uses aliases) ----
,
FILTER_PFT                            as ( SELECT * FROM    RENAME_PFT 
                                            WHERE FNCL_TRAN_TYP_ID IN (7, 8, 10, 21, 22, 40, 6303001, 6303021, 6303023, 6303024, 6303025, 6303026, 6303027, 
                                                                       6303028, 6303030, 6303031, 6303032, 6303033, 6303034, 6303042, 6303043, 6303061, 6303062, 
                                                                       6303073, 6303093, 6303096, 6303097, 6303098, 6303099, 6303100, 6303101, 6303102, 6303103, 
                                                                       6303104, 6303105, 6303112, 6303113, 6303115, 6303122, 6303123, 6303124, 6303126, 6307001, 
                                                                       6307003, 6307005, 6307007, 6330206, 6330207, 6330208, 6330209, 6330210, 6332015, 6332018, 
                                                                       6333212, 6333228, 6333229, 6333230, 6333231, 6333232, 6333233)  ),
FILTER_P                              as ( SELECT * FROM    RENAME_P 
                                            WHERE AGRE_TYP_CD = 'PLCY'  ),
FILTER_PP                             as ( SELECT * FROM    RENAME_PP   ),
FILTER_U                              as ( SELECT * FROM    RENAME_U   ),
FILTER_X                              as ( SELECT * FROM    RENAME_X   ),
FILTER_BSDA                           as ( SELECT * FROM    RENAME_BSDA   ),
FILTER_BSD                            as ( SELECT * FROM    RENAME_BSD   ),
FILTER_AG                             as ( SELECT * FROM    RENAME_AG   ),
FILTER_RPT                            as ( SELECT * FROM    RENAME_RPT 
                                            WHERE RPT_VOID_IND = 'N'  ),
FILTER_PAUD                           as ( SELECT * FROM    RENAME_PAUD 
                                            WHERE PAUD_VOID_IND = 'N' AND PLCY_PRD_AUDT_DTL_EFF_DT >= '2015-07-01'  ),
FILTER_PT                             as ( SELECT * FROM    RENAME_PT 
                                            WHERE PT_VOID_IND = 'N' AND PT_CTL_ELEM_TYP_CD = 'PLCY_TYP'  ),
FILTER_PAY                            as ( SELECT * FROM    RENAME_PAY 
                                            WHERE PAY_VOID_IND = 'N' AND PAY_CTL_ELEM_TYP_CD = 'PYMT_PLN'  ),
FILTER_FREQ                           as ( SELECT * FROM    RENAME_FREQ 
                                            WHERE FREQ_VOID_IND = 'N' AND FREQ_CTL_ELEM_TYP_CD = 'RPT_FREQ'  ),
FILTER_AUDT                           as ( SELECT * FROM    RENAME_AUDT 
                                            WHERE AUDT_VOID_IND = 'N' AND AUDT_CTL_ELEM_TYP_CD = 'AUDT_TYP'  ),
FILTER_ELT                            as ( SELECT * FROM    RENAME_ELT 
                                            WHERE ELT_VOID_IND = 'N' AND ELT_CTL_ELEM_TYP_CD = 'EMP_LS_PLCY_TYP'  ),
FILTER_PROF                           as ( SELECT * FROM    RENAME_PROF 
                                            WHERE PLCY_PRFL_VOID_IND = 'N' AND PRFL_STMT_ID = 6303027  ),
FILTER_PREM1                          as ( SELECT * FROM    RENAME_PREM1 
                                            WHERE PREM1_VOID_IND = 'N' AND PREM1_PREM_TYP_CD = 'E'  ),
FILTER_WCP1                           as ( SELECT * FROM    RENAME_WCP1 
                                            WHERE WCP1_WC_COV_PREM_VOID_IND = 'N'  ),
FILTER_PREM2                          as ( SELECT * FROM    RENAME_PREM2 
                                            WHERE PREM2_VOID_IND = 'N' AND PREM2_PREM_TYP_CD = 'R'  ),
FILTER_WCP2                           as ( SELECT * FROM    RENAME_WCP2 
                                            WHERE WCP2_WC_COV_PREM_VOID_IND = 'N'  ),
FILTER_PREM3                          as ( SELECT * FROM    RENAME_PREM3 
                                            WHERE PREM3_VOID_IND = 'N' AND PREM3_PREM_TYP_CD = 'A'  ),
FILTER_WCP3                           as ( SELECT * FROM    RENAME_WCP3 
                                            WHERE WCP3_WC_COV_PREM_VOID_IND = 'N'  ),
FILTER_PSRH                           as ( SELECT * FROM    RENAME_PSRH 
                                            WHERE CRNT_PLCY_PRD_STS_RSN_IND = 'Y'  ),
---- JOIN LAYER ----

BSDA as ( SELECT * 
                                                          FROM  FILTER_BSDA
                                                          LEFT JOIN FILTER_BSD ON  FILTER_BSDA.BSDA_BILL_SCH_DTL_ID =  FILTER_BSD.BSD_BILL_SCH_DTL_ID  ),
X as ( SELECT * 
                                                          FROM  FILTER_X
                                                          LEFT JOIN BSDA ON  FILTER_X.X_BILL_SCH_DTL_AMT_ID = BSDA.BSDA_BILL_SCH_DTL_AMT_ID  ),
PREM1 as ( SELECT FILTER_PREM1.PREM1_PLCY_PRD_ID, sum(FILTER_WCP1.WC_COV_PREM_BS_VAL) AS sum_WCP1_WC_COV_PREM_BS_VAL
                                                          FROM  FILTER_PREM1
                                                          LEFT JOIN FILTER_WCP1 ON  FILTER_PREM1.PREM1_PREM_PRD_ID =  FILTER_WCP1.WCP1_PREM_PRD_ID
                GROUP BY FILTER_PREM1.PREM1_PLCY_PRD_ID  ),
PREM2 as ( SELECT FILTER_PREM2.PREM2_PLCY_PRD_ID, sum(FILTER_WCP2.WC_COV_PREM_BS_VAL) AS sum_WCP2_WC_COV_PREM_BS_VAL
                                                          FROM  FILTER_PREM2
                                                          LEFT JOIN FILTER_WCP2 ON  FILTER_PREM2.PREM2_PREM_PRD_ID =  FILTER_WCP2.WCP2_PREM_PRD_ID
                GROUP BY FILTER_PREM2.PREM2_PLCY_PRD_ID  ),
PREM3 as ( SELECT FILTER_PREM3.PREM3_PLCY_PRD_ID, sum(FILTER_WCP3.WC_COV_PREM_BS_VAL) AS sum_WCP3_WC_COV_PREM_BS_VAL 
                                                          FROM  FILTER_PREM3
                                                          LEFT JOIN FILTER_WCP3 ON  FILTER_PREM3.PREM3_PREM_PRD_ID =  FILTER_WCP3.WCP3_PREM_PRD_ID
                GROUP BY FILTER_PREM3.PREM3_PLCY_PRD_ID  ),
PFT as ( SELECT * 
                 FROM  FILTER_PFT
                 INNER JOIN FILTER_P ON  FILTER_PFT.AGRE_ID =  FILTER_P.P_AGRE_ID 
                 LEFT JOIN FILTER_PP ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_PP.PP_PLCY_PRD_ID 
                 LEFT JOIN FILTER_U ON  FILTER_PFT.AUDIT_USER_ID_CREA =  FILTER_U.USER_ID 
                 LEFT JOIN X ON  FILTER_PFT.PFT_ID = X.X_PFT_ID 
                 LEFT JOIN FILTER_AG ON  FILTER_PFT.PFT_ID =  FILTER_AG.AG_PFT_ID 
                 LEFT JOIN FILTER_RPT ON  FILTER_PFT.PYRL_RPT_ID =  FILTER_RPT.RPT_PYRL_RPT_ID 
                 LEFT JOIN FILTER_PAUD ON  FILTER_PFT.PLCY_PRD_AUDT_DTL_ID =  FILTER_PAUD.PAUD_PLCY_PRD_AUDT_DTL_ID 
                 LEFT JOIN FILTER_PT ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_PT.PT_PLCY_PRD_ID 
                 LEFT JOIN FILTER_PAY ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_PAY.PAY_PLCY_PRD_ID 
                 LEFT JOIN FILTER_FREQ ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_FREQ.FREQ_PLCY_PRD_ID 
                 LEFT JOIN FILTER_AUDT ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_AUDT.AUDT_PLCY_PRD_ID 
                 LEFT JOIN FILTER_ELT ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_ELT.ELT_PLCY_PRD_ID 
                 LEFT JOIN FILTER_PROF ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_PROF.PROF_PLCY_PRD_ID 
                 LEFT JOIN PREM1 ON  FILTER_PFT.PLCY_PRD_ID = PREM1.PREM1_PLCY_PRD_ID 
                 LEFT JOIN PREM2 ON  FILTER_PFT.PLCY_PRD_ID = PREM2.PREM2_PLCY_PRD_ID 
                 LEFT JOIN PREM3 ON  FILTER_PFT.PLCY_PRD_ID = PREM3.PREM3_PLCY_PRD_ID 
                 LEFT JOIN FILTER_PSRH ON  FILTER_PFT.PLCY_PRD_ID =  FILTER_PSRH.PSRH_PLCY_PRD_ID 
                 LEFT JOIN (SELECT DISTINCT  PP_AGRE_ID, 
                                             POLICY_TYPE_CODE AS AA_CTL_ELEM_SUB_TYP_CD, 
                                             POLICY_TYPE_DESC AS AA_CTL_ELEM_SUB_TYP_NM FROM FILTER_PP a
                INNER JOIN FILTER_PT b on a.PP_PLCY_PRD_ID = b.PT_PLCY_PRD_ID and a.VOID_IND = 'N') AA ON AA.PP_AGRE_ID = FILTER_PFT.AGRE_ID
                                ),
-- ETL LAYER -----
 ETL AS ( SELECT md5(cast(
    
    coalesce(cast(PFT_ID as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY
,PLCY_NO
,EMPLOYER_CUSTOMER_ID
,EMPLOYER_CUSTOMER_NUMBER
,PLCY_PRD_ID
,PLCY_PRD_EFF_DT
,PLCY_PRD_END_DT
,NEW_POLICY_IND
,COALESCE(POLICY_TYPE_CODE, AA_CTL_ELEM_SUB_TYP_CD) as POLICY_TYPE_CODE
,COALESCE(POLICY_TYPE_DESC, AA_CTL_ELEM_SUB_TYP_NM) as POLICY_TYPE_DESC
,CASE WHEN PLCY_STS_TYP_CD IS NULL THEN 'N/A' ELSE PLCY_STS_TYP_CD END as PLCY_STS_TYP_CD
,CASE WHEN PLCY_STS_TYP_NM IS NULL THEN 'N/A' ELSE PLCY_STS_TYP_NM END as PLCY_STS_TYP_NM
,CASE WHEN PLCY_STS_RSN_TYP_CD IS NULL THEN 'N/A' ELSE PLCY_STS_RSN_TYP_CD END as PLCY_STS_RSN_TYP_CD
,CASE WHEN PLCY_STS_RSN_TYP_NM IS NULL THEN 'N/A' ELSE PLCY_STS_RSN_TYP_NM END as PLCY_STS_RSN_TYP_NM
,CASE WHEN PAYMENT_PLAN_TYPE_CODE IS NULL THEN 'N/A' ELSE PAYMENT_PLAN_TYPE_CODE END as PAYMENT_PLAN_TYPE_CODE
,CASE WHEN PAYMENT_PLAN_TYPE_DESC IS NULL THEN 'NO PAYMENT PLAN' ELSE PAYMENT_PLAN_TYPE_DESC END as PAYMENT_PLAN_TYPE_DESC 
,CASE WHEN REPORTING_FREQUENCY_TYPE_CODE IS NULL THEN 'N/A' ELSE REPORTING_FREQUENCY_TYPE_CODE END as REPORTING_FREQUENCY_TYPE_CODE
,CASE WHEN REPORTING_FREQUENCY_TYPE_DESC IS NULL THEN 'NO REPORTING FREQUENCY' ELSE REPORTING_FREQUENCY_TYPE_DESC END as REPORTING_FREQUENCY_TYPE_DESC  
,CASE WHEN AUDIT_TYPE_CODE IS NULL THEN 'N/A' ELSE AUDIT_TYPE_CODE END as AUDIT_TYPE_CODE 
,CASE WHEN AUDIT_TYPE_DESC IS NULL THEN 'NO AUDIT TYPE' ELSE AUDIT_TYPE_DESC END as AUDIT_TYPE_DESC   
,CASE WHEN EMPLOYEE_LEASING_TYPE_CODE IS NULL THEN 'N/A' ELSE EMPLOYEE_LEASING_TYPE_CODE END as EMPLOYEE_LEASING_TYPE_CODE  
,CASE WHEN EMPLOYEE_LEASING_TYPE_DESC IS NULL THEN 'NO EMPLOYEE LEASING TYPE' ELSE EMPLOYEE_LEASING_TYPE_DESC END as EMPLOYEE_LEASING_TYPE_DESC 
,PFT_ID
,PFT_DT
,PFT_DUE_DT
,PFT_AMT
,PFT_DRV_BAL_AMT
,FNCL_TRAN_TYP_ID
,FNCL_TRAN_TYP_CD
,FNCL_TRAN_TYP_NM
,CASE WHEN FNCL_TRAN_SUB_TYP_CD IS NULL THEN 'N/A' ELSE FNCL_TRAN_SUB_TYP_CD END as FNCL_TRAN_SUB_TYP_CD
,CASE WHEN FNCL_TRAN_SUB_TYP_NM IS NULL THEN 'N/A' ELSE FNCL_TRAN_SUB_TYP_NM END as FNCL_TRAN_SUB_TYP_NM
,BILL_NO
,LINE_NO
,CERTIFIED_AG_DATE
,INVC_ID
,CUST_ID
,CASE WHEN PFT_CMT IS NULL THEN 'N/A' ELSE PFT_CMT END as PFT_CMT 
,CASE WHEN PYRL_RPT_EST_RPT_IND IS NOT NULL THEN PYRL_RPT_EST_RPT_IND WHEN PLCY_AUDT_TYP_CD in ('ETU', 'EST') THEN 'Y' ELSE 'N' END as ESTIMATED_PREMIUM_IND
,POLICY_EMPLOYER_PAID_PROGRAM_IND 
--,COALESCE(POLICY_15K_PROGRAM_IND , 'N') AS POLICY_15K_PROGRAM_IND
,CASE WHEN sum_WCP1_WC_COV_PREM_BS_VAL = 0 THEN 'Y' ELSE 'N' END AS ESTIMATED_ZERO_PAYROLL_IND
,CASE WHEN COALESCE (sum_WCP3_WC_COV_PREM_BS_VAL,sum_WCP2_WC_COV_PREM_BS_VAL) = 0 THEN 'Y' ELSE 'N' END as REPORTED_ZERO_PAYROLL_IND
,AUDIT_USER_CREA_DTM
,AUDIT_USER_UPDT_DTM
,AUDIT_USER_ID_CREA
,CREATE_USER_LOGIN_NAME
,CREATE_USER_NAME
,PT_CTL_ELEM_TYP_CD
,PAY_CTL_ELEM_TYP_CD
,FREQ_CTL_ELEM_TYP_CD
,AUDT_CTL_ELEM_TYP_CD
,ELT_CTL_ELEM_TYP_CD
FROM PFT QUALIFY(ROW_NUMBER()OVER(PARTITION BY PFT_ID ORDER BY BILL_NO::NUMBER ASC , LINE_NO::NUMBER ASC))=1)

select *  from ETL