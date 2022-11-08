

---- SRC LAYER ----
WITH
SRC_PB as ( SELECT *     from     STAGING.DST_POLICY_BILLING ),
//SRC_PB as ( SELECT *     from     DST_POLICY_BILLING) ,

---- LOGIC LAYER ----


LOGIC_PB as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, PAYMENT_PLAN_TYPE_CODE                             as                             PAYMENT_PLAN_TYPE_CODE 
		, PAYMENT_PLAN_TYPE_DESC                             as                             PAYMENT_PLAN_TYPE_DESC 
		, REPORTING_FREQUENCY_TYPE_CODE                      as                      REPORTING_FREQUENCY_TYPE_CODE 
		, REPORTING_FREQUENCY_TYPE_DESC                      as                      REPORTING_FREQUENCY_TYPE_DESC 
		, AUDIT_TYPE_CODE                                    as                                    AUDIT_TYPE_CODE 
		, AUDIT_TYPE_DESC                                    as                                    AUDIT_TYPE_DESC 
		, EMPLOYEE_LEASING_TYPE_CODE                         as                         EMPLOYEE_LEASING_TYPE_CODE 
		, EMPLOYEE_LEASING_TYPE_DESC                         as                         EMPLOYEE_LEASING_TYPE_DESC 
		, POLICY_15K_PROGRAM_IND                             as                             POLICY_15K_PROGRAM_IND 
		, ESTIMATED_ZERO_PAYROLL_IND                         as                         ESTIMATED_ZERO_PAYROLL_IND 
		, REPORTED_ZERO_PAYROLL_IND                          as                          REPORTED_ZERO_PAYROLL_IND 
		, ESTIMATED_PREMIUM_IND                              as                              ESTIMATED_PREMIUM_IND 
		from SRC_PB
            )

---- RENAME LAYER ----
,

RENAME_PB as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, PAYMENT_PLAN_TYPE_CODE                             as                             PAYMENT_PLAN_TYPE_CODE
		, PAYMENT_PLAN_TYPE_DESC                             as                             PAYMENT_PLAN_TYPE_DESC
		, REPORTING_FREQUENCY_TYPE_CODE                      as                      REPORTING_FREQUENCY_TYPE_CODE
		, REPORTING_FREQUENCY_TYPE_DESC                      as                      REPORTING_FREQUENCY_TYPE_DESC
		, AUDIT_TYPE_CODE                                    as                                    AUDIT_TYPE_CODE
		, AUDIT_TYPE_DESC                                    as                                    AUDIT_TYPE_DESC
		, EMPLOYEE_LEASING_TYPE_CODE                         as                         EMPLOYEE_LEASING_TYPE_CODE
		, EMPLOYEE_LEASING_TYPE_DESC                         as                         EMPLOYEE_LEASING_TYPE_DESC
		, POLICY_15K_PROGRAM_IND                             as                             POLICY_15K_PROGRAM_IND
		, ESTIMATED_ZERO_PAYROLL_IND                         as                         ESTIMATED_ZERO_PAYROLL_IND
		, REPORTED_ZERO_PAYROLL_IND                          as                          REPORTED_ZERO_PAYROLL_IND
		, ESTIMATED_PREMIUM_IND                              as                              ESTIMATED_PREMIUM_IND 
				FROM     LOGIC_PB   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PB                             as ( SELECT * from    RENAME_PB   ),

---- JOIN LAYER ----

 JOIN_PB  as  ( SELECT * 
				FROM  FILTER_PB )
 SELECT * FROM  JOIN_PB