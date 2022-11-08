

      create or replace  table DEV_EDW.EDW_STG_POLICY_MART.FACT_EARNED_PREMIUM  as
      (

---- SRC LAYER ----
WITH
SRC_EP             as ( SELECT *     FROM     STAGING.DSV_EARNED_PREMIUM ),
SRC_EMP            as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_EMPLOYER ),
SRC_U              as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_USER ),
//SRC_EP             as ( SELECT *     FROM     DSV_EARNED_PREMIUM) ,
//SRC_EMP            as ( SELECT *     FROM     DIM_EMPLOYER) ,
//SRC_U              as ( SELECT *     FROM     DIM_USER) ,

---- LOGIC LAYER ----


LOGIC_EP as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                        UNIQUE_ID_KEY 
		, FINANCIAL_TRANSACTION_ID                           as                             FINANCIAL_TRANSACTION_ID
		, FINANCIAL_TRANSACTION_TYPE_ID                      as                        FINANCIAL_TRANSACTION_TYPE_ID
		, FINANCIAL_TRANSACTION_STATUS_CODE                  as                    FINANCIAL_TRANSACTION_STATUS_CODE
		, EMPLOYER_CUSTOMER_NUMBER                           as                             EMPLOYER_CUSTOMER_NUMBER
		, POLICY_PERIOD_EFFECTIVE_DATE                       as                         POLICY_PERIOD_EFFECTIVE_DATE
		, POLICY_PERIOD_END_DATE                             as                               POLICY_PERIOD_END_DATE
		, PEC_POLICY_IND                                     as                                       PEC_POLICY_IND
		, NEW_POLICY_IND                                     as                                       NEW_POLICY_IND
        , POLICY_PERIOD_EFFECTIVE_DATE || ' - ' || POLICY_PERIOD_END_DATE as                      POLICY_PERIOD_DESC
		, POLICY_TYPE_CODE               					 as                                     POLICY_TYPE_CODE
        , POLICY_STATUS_CODE                                 as                                   POLICY_STATUS_CODE
		, STATUS_REASON_CODE                                 as      							  STATUS_REASON_CODE
		, POLICY_ACTIVE_IND                                  as                                    POLICY_ACTIVE_IND
		, PAYMENT_PLAN_TYPE_CODE                             as                               PAYMENT_PLAN_TYPE_CODE
		, REPORTING_FREQUENCY_TYPE_CODE                      as                        REPORTING_FREQUENCY_TYPE_CODE
		, AUDIT_TYPE_CODE                                    as                                      AUDIT_TYPE_CODE
		, EMPLOYEE_LEASING_TYPE_CODE                         as                           EMPLOYEE_LEASING_TYPE_CODE
		--, POLICY_15K_PROGRAM_IND                             as                               POLICY_15K_PROGRAM_IND
		, POLICY_EMPLOYER_PAID_PROGRAM_IND                   as                     POLICY_EMPLOYER_PAID_PROGRAM_IND
		, ESTIMATED_ZERO_PAYROLL_IND                         as                           ESTIMATED_ZERO_PAYROLL_IND
		, REPORTED_ZERO_PAYROLL_IND                          as                            REPORTED_ZERO_PAYROLL_IND
		, ESTIMATED_PREMIUM_IND                              as                                ESTIMATED_PREMIUM_IND
		, BILLED_DATE                                        as                                          BILLED_DATE
		, PFT_COMMENT_TEXT                                   as                                     PFT_COMMENT_TEXT
		, PAYMENT_PLAN_TYPE_DESC                             as                               PAYMENT_PLAN_TYPE_DESC
        , REPORTING_FREQUENCY_TYPE_DESC                      as                        REPORTING_FREQUENCY_TYPE_DESC
        , AUDIT_TYPE_DESC                                    as                                      AUDIT_TYPE_DESC
        , EMPLOYEE_LEASING_TYPE_DESC                         as                           EMPLOYEE_LEASING_TYPE_DESC
		, CREATE_USER_LOGIN_NAME                             as                               CREATE_USER_LOGIN_NAME
		, case when BILLED_DATE is null then -1
			when replace(cast(BILLED_DATE::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
			when replace(cast(BILLED_DATE::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
			else replace(cast(BILLED_DATE::DATE as varchar),'-','')::INTEGER 
		END 												 as 								     BILLED_DATE_KEY
		, case when BILL_DUE_DATE is null then -1
			when replace(cast(BILL_DUE_DATE::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
			when replace(cast(BILL_DUE_DATE::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
			else replace(cast(BILL_DUE_DATE::DATE as varchar),'-','')::INTEGER 
		END 												 as 								   BILL_DUE_DATE_KEY
		, case when PAID_DATE is null then -1
			when replace(cast(PAID_DATE::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
			when replace(cast(PAID_DATE::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
			else replace(cast(PAID_DATE::DATE as varchar),'-','')::INTEGER 
		END 												 as 								       PAID_DATE_KEY                                    
		, case when CERTIFIED_AG_DATE is null then -1
			when replace(cast(CERTIFIED_AG_DATE::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
			when replace(cast(CERTIFIED_AG_DATE::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
			else replace(cast(CERTIFIED_AG_DATE::DATE as varchar),'-','')::INTEGER 
		END 												 as 							   CERTIFIED_AG_DATE_KEY 
		, POLICY_NUMBER                                      as                                        POLICY_NUMBER 
		, INSTALLMENT_NUMBER                                 as                                   INSTALLMENT_NUMBER 
		, EARNED_AMOUNT                                      as                                        EARNED_AMOUNT 
		, PAID_AMOUNT                                        as                                          PAID_AMOUNT 
		, BALANCE_AMOUNT                                     as                                       BALANCE_AMOUNT 
		FROM SRC_EP
            ),

LOGIC_EMP as ( SELECT 
		  CUSTOMER_NUMBER                                   as                                    CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                             as                              RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                   as                                    RECORD_END_DATE
		, EMPLOYER_HKEY                                     as                                      EMPLOYER_HKEY
		FROM SRC_EMP
            ),

LOGIC_U as ( SELECT 
		  USER_HKEY                                          as                                          USER_HKEY
		, USER_LOGIN_NAME                                    as                                     USER_LOGIN_NAME                      
		FROM SRC_U
            )

---- RENAME LAYER ----
,

RENAME_EP         as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                        UNIQUE_ID_KEY
		, FINANCIAL_TRANSACTION_ID                           as                             FINANCIAL_TRANSACTION_ID
		, FINANCIAL_TRANSACTION_TYPE_ID                      as                        FINANCIAL_TRANSACTION_TYPE_ID
		, FINANCIAL_TRANSACTION_STATUS_CODE                  as                    FINANCIAL_TRANSACTION_STATUS_CODE
		, EMPLOYER_CUSTOMER_NUMBER                           as                             EMPLOYER_CUSTOMER_NUMBER
		, POLICY_PERIOD_EFFECTIVE_DATE                       as                         POLICY_PERIOD_EFFECTIVE_DATE
		, POLICY_PERIOD_END_DATE                             as                               POLICY_PERIOD_END_DATE
		, PEC_POLICY_IND                                     as                                       PEC_POLICY_IND
		, NEW_POLICY_IND                                     as                                       NEW_POLICY_IND
        , POLICY_PERIOD_DESC                                 as                                   POLICY_PERIOD_DESC
		, POLICY_TYPE_CODE               					 as                                     POLICY_TYPE_CODE
        , POLICY_STATUS_CODE                                 as                                   POLICY_STATUS_CODE
		, STATUS_REASON_CODE                                 as      							  STATUS_REASON_CODE
		, POLICY_ACTIVE_IND                                  as                                    POLICY_ACTIVE_IND
		, PAYMENT_PLAN_TYPE_CODE                             as                               PAYMENT_PLAN_TYPE_CODE
		, REPORTING_FREQUENCY_TYPE_CODE                      as                        REPORTING_FREQUENCY_TYPE_CODE
		, AUDIT_TYPE_CODE                                    as                                      AUDIT_TYPE_CODE
		, EMPLOYEE_LEASING_TYPE_CODE                         as                           EMPLOYEE_LEASING_TYPE_CODE
		--, POLICY_15K_PROGRAM_IND                             as                               POLICY_15K_PROGRAM_IND
	    , POLICY_EMPLOYER_PAID_PROGRAM_IND                   as                     POLICY_EMPLOYER_PAID_PROGRAM_IND
		, ESTIMATED_ZERO_PAYROLL_IND                         as                           ESTIMATED_ZERO_PAYROLL_IND
		, REPORTED_ZERO_PAYROLL_IND                          as                            REPORTED_ZERO_PAYROLL_IND
		, ESTIMATED_PREMIUM_IND                              as                                ESTIMATED_PREMIUM_IND
		, BILLED_DATE                                        as                                          BILLED_DATE
		, BILLED_DATE_KEY                                    as                                      BILLED_DATE_KEY
		, BILL_DUE_DATE_KEY                                  as                                    BILL_DUE_DATE_KEY
		, PAID_DATE_KEY                                      as                                        PAID_DATE_KEY
		, CERTIFIED_AG_DATE_KEY                              as                             CERTIFIED_TO_AG_DATE_KEY
		, POLICY_NUMBER                                      as                                        POLICY_NUMBER
		, INSTALLMENT_NUMBER                                 as                                   INSTALLMENT_NUMBER
		, EARNED_AMOUNT                                      as                                        EARNED_AMOUNT
		, PAID_AMOUNT                                        as                                          PAID_AMOUNT
		, BALANCE_AMOUNT                                     as                                       BALANCE_AMOUNT
		, PFT_COMMENT_TEXT                                   as                                     PFT_COMMENT_TEXT
		, PAYMENT_PLAN_TYPE_DESC                             as                               PAYMENT_PLAN_TYPE_DESC
        , REPORTING_FREQUENCY_TYPE_DESC                      as                        REPORTING_FREQUENCY_TYPE_DESC
        , AUDIT_TYPE_DESC                                    as                                      AUDIT_TYPE_DESC
        , EMPLOYEE_LEASING_TYPE_DESC                         as                           EMPLOYEE_LEASING_TYPE_DESC
		, CREATE_USER_LOGIN_NAME                             as                               CREATE_USER_LOGIN_NAME
				FROM     LOGIC_EP   ), 
RENAME_EMP        as ( SELECT 
		  EMPLOYER_HKEY                                      as                                        EMPLOYER_HKEY
		, RECORD_EFFECTIVE_DATE                              as                                RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                   as                                       RECORD_END_DATE
		, CUSTOMER_NUMBER                                    as                                      CUSTOMER_NUMBER 
				FROM     LOGIC_EMP   ), 
RENAME_U          as ( SELECT 
		  USER_HKEY                                          as             FINANCIAL_TRANSACTION_CREATE_USER_HKEY 
		, USER_LOGIN_NAME                                    as                                     USER_LOGIN_NAME
				FROM     LOGIC_U   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EP                             as ( SELECT * FROM    RENAME_EP   ),
FILTER_EMP                            as ( SELECT * FROM    RENAME_EMP   ),
FILTER_U                              as ( SELECT * FROM    RENAME_U   ),

---- JOIN LAYER ----

EP as ( SELECT * 
				FROM  FILTER_EP
				INNER JOIN FILTER_EMP ON  FILTER_EP.EMPLOYER_CUSTOMER_NUMBER =  FILTER_EMP.CUSTOMER_NUMBER AND FILTER_EP.BILLED_DATE BETWEEN 
				 FILTER_EMP.RECORD_EFFECTIVE_DATE AND COALESCE(FILTER_EMP.RECORD_END_DATE,'2999-12-31') 
				LEFT JOIN FILTER_U ON  FILTER_EP.CREATE_USER_LOGIN_NAME =  FILTER_U.USER_LOGIN_NAME  ),

--------ETL LAYER---------------
 ETL AS ( SELECT FINANCIAL_TRANSACTION_ID
,md5(cast(
    
    coalesce(cast(CUSTOMER_NUMBER as 
    varchar
), '') || '-' || coalesce(cast(RECORD_EFFECTIVE_DATE as 
    varchar
), '')

 as 
    varchar
)) as EMPLOYER_HKEY
,CASE WHEN nullif(array_to_string(array_construct_compact( POLICY_PERIOD_EFFECTIVE_DATE,POLICY_PERIOD_END_DATE,PEC_POLICY_IND,NEW_POLICY_IND,POLICY_PERIOD_DESC ),''),'') is NULL
			then MD5( '99998' ) ELSE md5(cast(
    
    coalesce(cast(POLICY_PERIOD_EFFECTIVE_DATE as 
    varchar
), '') || '-' || coalesce(cast(POLICY_PERIOD_END_DATE as 
    varchar
), '') || '-' || coalesce(cast(PEC_POLICY_IND as 
    varchar
), '') || '-' || coalesce(cast(NEW_POLICY_IND as 
    varchar
), '') || '-' || coalesce(cast(POLICY_PERIOD_DESC as 
    varchar
), '')

 as 
    varchar
)) 
				END as POLICY_PERIOD_HKEY
,coalesce(md5(cast(
    
    coalesce(cast(POLICY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(POLICY_STATUS_CODE as 
    varchar
), '') || '-' || coalesce(cast(STATUS_REASON_CODE as 
    varchar
), '') || '-' || coalesce(cast(POLICY_ACTIVE_IND as 
    varchar
), '')

 as 
    varchar
)),md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) as POLICY_STANDING_HKEY
,md5(cast(
    
    coalesce(cast(PAYMENT_PLAN_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(REPORTING_FREQUENCY_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(AUDIT_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYEE_LEASING_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(POLICY_EMPLOYER_PAID_PROGRAM_IND as 
    varchar
), '') || '-' || coalesce(cast(ESTIMATED_ZERO_PAYROLL_IND as 
    varchar
), '') || '-' || coalesce(cast(REPORTED_ZERO_PAYROLL_IND as 
    varchar
), '') || '-' || coalesce(cast(ESTIMATED_PREMIUM_IND as 
    varchar
), '')

 as 
    varchar
)) as POLICY_BILLING_HKEY
,md5(cast(
    
    coalesce(cast(FINANCIAL_TRANSACTION_TYPE_ID as 
    varchar
), '')

 as 
    varchar
)) as FINANCIAL_TRANSACTION_TYPE_HKEY
,CASE WHEN  FINANCIAL_TRANSACTION_STATUS_CODE IS NULL THEN MD5('88888') ELSE md5(cast(
    
    coalesce(cast(FINANCIAL_TRANSACTION_STATUS_CODE as 
    varchar
), '')

 as 
    varchar
)) END as FINANCIAL_TRANSACTION_STATUS_HKEY
,BILLED_DATE_KEY
,BILL_DUE_DATE_KEY
,PAID_DATE_KEY
,CERTIFIED_TO_AG_DATE_KEY
,CASE WHEN PFT_COMMENT_TEXT IS NULL THEN MD5('N/A') ELSE md5(cast(
    
    coalesce(cast(PFT_COMMENT_TEXT as 
    varchar
), '')

 as 
    varchar
)) END AS  FINANCIAL_TRANSACTION_COMMENT_HKEY
,coalesce(FINANCIAL_TRANSACTION_CREATE_USER_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS FINANCIAL_TRANSACTION_CREATE_USER_HKEY
,POLICY_NUMBER
,INSTALLMENT_NUMBER
,EARNED_AMOUNT
,CAST(NVL(PAID_AMOUNT,0)AS NUMERIC(32,2)) AS PAID_AMOUNT
,BALANCE_AMOUNT
,CURRENT_TIMESTAMP AS  LOAD_DATETIME
,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
,'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
FROM EP)
SELECT * FROM ETL
      );
    