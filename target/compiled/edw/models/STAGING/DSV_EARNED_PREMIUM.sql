

---- SRC LAYER ----
WITH
SRC_EPB            as ( SELECT *     FROM     STAGING.DST_EARNED_PREMIUM_BILLS ),
SRC_EPP            as ( SELECT *     FROM     STAGING.DST_EARNED_PREMIUM_PAYMENTS ),
//SRC_EPB            as ( SELECT *     FROM     DST_EARNED_PREMIUM_BILLS) ,
//SRC_EPP            as ( SELECT *     FROM     DST_EARNED_PREMIUM_PAYMENTS) ,

---- LOGIC LAYER ----


LOGIC_EPB as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, PLCY_NO                                            as                                            PLCY_NO 
		, EMPLOYER_CUSTOMER_ID                               as                               EMPLOYER_CUSTOMER_ID 
		, EMPLOYER_CUSTOMER_NUMBER                           as                           EMPLOYER_CUSTOMER_NUMBER 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT 
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, POLICY_TYPE_DESC                                   as                                   POLICY_TYPE_DESC 
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD 
		, PLCY_STS_TYP_NM                                    as                                    PLCY_STS_TYP_NM 
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD 
		, PLCY_STS_RSN_TYP_NM                                as                                PLCY_STS_RSN_TYP_NM                                                 
		, PAYMENT_PLAN_TYPE_CODE                             as                             PAYMENT_PLAN_TYPE_CODE 
		, PAYMENT_PLAN_TYPE_DESC                             as                             PAYMENT_PLAN_TYPE_DESC 
		, REPORTING_FREQUENCY_TYPE_CODE                      as                      REPORTING_FREQUENCY_TYPE_CODE 
		, REPORTING_FREQUENCY_TYPE_DESC                      as                      REPORTING_FREQUENCY_TYPE_DESC 
		, AUDIT_TYPE_CODE                                    as                                    AUDIT_TYPE_CODE 
		, AUDIT_TYPE_DESC                                    as                                    AUDIT_TYPE_DESC 
		, EMPLOYEE_LEASING_TYPE_CODE                         as                         EMPLOYEE_LEASING_TYPE_CODE 
		, EMPLOYEE_LEASING_TYPE_DESC                         as                         EMPLOYEE_LEASING_TYPE_DESC 
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
		, BILL_NO                                            as                                            BILL_NO 
		, LINE_NO                                            as                                            LINE_NO 
		, CERTIFIED_AG_DATE                                  as                                  CERTIFIED_AG_DATE 
		, PFT_CMT                                            as                                            PFT_CMT 
		, ESTIMATED_PREMIUM_IND                              as                              ESTIMATED_PREMIUM_IND 
		, POLICY_EMPLOYER_PAID_PROGRAM_IND                             as                             POLICY_EMPLOYER_PAID_PROGRAM_IND 
		, ESTIMATED_ZERO_PAYROLL_IND                         as                         ESTIMATED_ZERO_PAYROLL_IND 
		, REPORTED_ZERO_PAYROLL_IND                          as                          REPORTED_ZERO_PAYROLL_IND 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, CREATE_USER_LOGIN_NAME                             as                             CREATE_USER_LOGIN_NAME 
		, CREATE_USER_NAME                                   as                                   CREATE_USER_NAME 
		, NEW_POLICY_IND                                     as                       NEW_POLICY_IND
		FROM SRC_EPB
            ),

LOGIC_EPP as ( SELECT 
		  PFTA_DT                                            as                                            PFTA_DT 
		, PFTA_AMT                                           as                                           PFTA_AMT 
		, BILL_PFT_ID                                        as                                        BILL_PFT_ID 
		FROM SRC_EPP
            )

---- RENAME LAYER ----
,

RENAME_EPB        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, PLCY_NO                                            as                                      POLICY_NUMBER
		, EMPLOYER_CUSTOMER_ID                               as                               EMPLOYER_CUSTOMER_ID
		, EMPLOYER_CUSTOMER_NUMBER                           as                           EMPLOYER_CUSTOMER_NUMBER
		, PLCY_PRD_ID                                        as                                   POLICY_PERIOD_ID
		, PLCY_PRD_EFF_DT                                    as                       POLICY_PERIOD_EFFECTIVE_DATE
		, PLCY_PRD_END_DT                                    as                             POLICY_PERIOD_END_DATE
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, POLICY_TYPE_DESC                                   as                                   POLICY_TYPE_DESC
		, PLCY_STS_TYP_CD                                    as                                 POLICY_STATUS_CODE
		, PLCY_STS_TYP_NM                                    as                                 POLICY_STATUS_DESC
		, PLCY_STS_RSN_TYP_CD                                as                                 STATUS_REASON_CODE
		, PLCY_STS_RSN_TYP_NM                                as                                 STATUS_REASON_DESC
		, PAYMENT_PLAN_TYPE_CODE                             as                             PAYMENT_PLAN_TYPE_CODE
		, PAYMENT_PLAN_TYPE_DESC                             as                             PAYMENT_PLAN_TYPE_DESC
		, REPORTING_FREQUENCY_TYPE_CODE                      as                      REPORTING_FREQUENCY_TYPE_CODE
		, REPORTING_FREQUENCY_TYPE_DESC                      as                      REPORTING_FREQUENCY_TYPE_DESC
		, AUDIT_TYPE_CODE                                    as                                    AUDIT_TYPE_CODE
		, AUDIT_TYPE_DESC                                    as                                    AUDIT_TYPE_DESC
		, EMPLOYEE_LEASING_TYPE_CODE                         as                         EMPLOYEE_LEASING_TYPE_CODE
		, EMPLOYEE_LEASING_TYPE_DESC                         as                         EMPLOYEE_LEASING_TYPE_DESC
		, PFT_ID                                             as                           FINANCIAL_TRANSACTION_ID
		, PFT_DT                                             as                                        BILLED_DATE
		, PFT_DUE_DT                                         as                                      BILL_DUE_DATE
		, PFT_AMT                                            as                                      EARNED_AMOUNT
		, PFT_DRV_BAL_AMT                                    as                                     BALANCE_AMOUNT
		, FNCL_TRAN_TYP_ID                                   as                      FINANCIAL_TRANSACTION_TYPE_ID
		, FNCL_TRAN_TYP_CD                                   as                    FINANCIAL_TRANSACTION_TYPE_CODE
		, FNCL_TRAN_TYP_NM                                   as                    FINANCIAL_TRANSACTION_TYPE_DESC
		, FNCL_TRAN_SUB_TYP_CD                               as                  FINANCIAL_TRANSACTION_STATUS_CODE
		, FNCL_TRAN_SUB_TYP_NM                               as                  FINANCIAL_TRANSACTION_STATUS_DESC
		, BILL_NO                                            as                                 INSTALLMENT_NUMBER
		, LINE_NO                                            as                                        LINE_NUMBER
		, CERTIFIED_AG_DATE                                  as                                  CERTIFIED_AG_DATE
		, PFT_CMT                                            as                                   PFT_COMMENT_TEXT
		, ESTIMATED_PREMIUM_IND                              as                              ESTIMATED_PREMIUM_IND
		, POLICY_EMPLOYER_PAID_PROGRAM_IND                             as                             POLICY_EMPLOYER_PAID_PROGRAM_IND
		, ESTIMATED_ZERO_PAYROLL_IND                         as                         ESTIMATED_ZERO_PAYROLL_IND
		, REPORTED_ZERO_PAYROLL_IND                          as                          REPORTED_ZERO_PAYROLL_IND
		, AUDIT_USER_CREA_DTM                                as                                     PFT_CREATE_DTM
		, AUDIT_USER_UPDT_DTM                                as                                     PFT_UPDATE_DTM
		, AUDIT_USER_ID_CREA                                 as                                 PFT_CREATE_USER_ID
		, CREATE_USER_LOGIN_NAME                             as                             CREATE_USER_LOGIN_NAME
		, CREATE_USER_NAME                                   as                                   CREATE_USER_NAME 
		, NEW_POLICY_IND                                     as                           NEW_POLICY_IND		
				FROM     LOGIC_EPB   ), 
RENAME_EPP        as ( SELECT 
		  PFTA_DT                                            as                                          PAID_DATE
		, PFTA_AMT                                           as                                        PAID_AMOUNT
		, BILL_PFT_ID                                        as                                        BILL_PFT_ID 
				FROM     LOGIC_EPP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EPB                            as ( SELECT * FROM    RENAME_EPB   ),
FILTER_EPP                            as ( select BILL_PFT_ID, max(PAID_DATE) as PAID_DATE, sum(PAID_AMOUNT) as PAID_AMOUNT
from RENAME_EPP group by BILL_PFT_ID  ),

---- JOIN LAYER ----

EPB as ( SELECT * 
				FROM  FILTER_EPB
				LEFT JOIN FILTER_EPP ON  FILTER_EPB.FINANCIAL_TRANSACTION_ID =  FILTER_EPP.BILL_PFT_ID  ),

-------ETL LAYER------
 ETL AS ( SELECT 
 UNIQUE_ID_KEY
,POLICY_NUMBER
,EMPLOYER_CUSTOMER_ID
,EMPLOYER_CUSTOMER_NUMBER
,POLICY_PERIOD_ID
,POLICY_PERIOD_EFFECTIVE_DATE
,POLICY_PERIOD_END_DATE
,POLICY_TYPE_CODE
,POLICY_TYPE_DESC
,CASE WHEN POLICY_TYPE_CODE = 'PEC' THEN 'Y' ELSE 'N' END AS PEC_POLICY_IND
,NEW_POLICY_IND
--,CASE WHEN POLICY_PERIOD_EFFECTIVE_DATE = MIN(POLICY_PERIOD_EFFECTIVE_DATE) OVER (PARTITION BY POLICY_NUMBER) THEN 'Y' ELSE 'N' END AS NEW_POLICY_IND
,POLICY_STATUS_CODE
,POLICY_STATUS_DESC
,STATUS_REASON_CODE
,STATUS_REASON_DESC
,CASE WHEN POLICY_STATUS_CODE in ('EXP', 'ACT') THEN 'Y' WHEN POLICY_STATUS_CODE IS NULL THEN 'U' ELSE 'N' END AS POLICY_ACTIVE_IND
,PAYMENT_PLAN_TYPE_CODE
,PAYMENT_PLAN_TYPE_DESC
,REPORTING_FREQUENCY_TYPE_CODE
,REPORTING_FREQUENCY_TYPE_DESC
,AUDIT_TYPE_CODE
,AUDIT_TYPE_DESC
,EMPLOYEE_LEASING_TYPE_CODE
,EMPLOYEE_LEASING_TYPE_DESC
,FINANCIAL_TRANSACTION_ID
,BILLED_DATE
,BILL_DUE_DATE
,EARNED_AMOUNT
,BALANCE_AMOUNT
,FINANCIAL_TRANSACTION_TYPE_ID
,FINANCIAL_TRANSACTION_TYPE_CODE
,FINANCIAL_TRANSACTION_TYPE_DESC
,FINANCIAL_TRANSACTION_STATUS_CODE
,FINANCIAL_TRANSACTION_STATUS_DESC
,INSTALLMENT_NUMBER
,LINE_NUMBER
,CERTIFIED_AG_DATE
,PFT_COMMENT_TEXT
,ESTIMATED_PREMIUM_IND
,POLICY_EMPLOYER_PAID_PROGRAM_IND
,ESTIMATED_ZERO_PAYROLL_IND
,REPORTED_ZERO_PAYROLL_IND
,PFT_CREATE_DTM
,PFT_UPDATE_DTM
,PFT_CREATE_USER_ID
,CREATE_USER_LOGIN_NAME
,CREATE_USER_NAME
,PAID_DATE
,PAID_AMOUNT
FROM EPB)

SELECT * FROM ETL