

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(PAYMENT_PLAN_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PAYMENT_PLAN_TYPE_DESC, 
     last_value(REPORTING_FREQUENCY_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as REPORTING_FREQUENCY_TYPE_DESC, 
     last_value(AUDIT_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as AUDIT_TYPE_DESC, 
     last_value(EMPLOYEE_LEASING_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EMPLOYEE_LEASING_TYPE_DESC, 
     last_value(POLICY_15K_PROGRAM_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as POLICY_15K_PROGRAM_IND, 
     last_value(ESTIMATED_ZERO_PAYROLL_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ESTIMATED_ZERO_PAYROLL_IND, 
     last_value(REPORTED_ZERO_PAYROLL_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as REPORTED_ZERO_PAYROLL_IND, 
     last_value(ESTIMATED_PREMIUM_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ESTIMATED_PREMIUM_IND
     
	FROM EDW_STAGING.DIM_POLICY_BILLING_SCDALL_STEP2),

ETL AS( 
SELECT
     md5(cast(
    
    coalesce(cast(PAYMENT_PLAN_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(REPORTING_FREQUENCY_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(AUDIT_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYEE_LEASING_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(POLICY_15K_PROGRAM_IND as 
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
)) As POLICY_BILLING_HKEY
    , UNIQUE_ID_KEY AS UNIQUE_ID_KEY
    , PAYMENT_PLAN_TYPE_DESC
    , REPORTING_FREQUENCY_TYPE_DESC
    , AUDIT_TYPE_DESC
    , EMPLOYEE_LEASING_TYPE_DESC
    , POLICY_15K_PROGRAM_IND
    , ESTIMATED_ZERO_PAYROLL_IND
    , REPORTED_ZERO_PAYROLL_IND
    , ESTIMATED_PREMIUM_IND
    , CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
)
SELECT * FROM ETL