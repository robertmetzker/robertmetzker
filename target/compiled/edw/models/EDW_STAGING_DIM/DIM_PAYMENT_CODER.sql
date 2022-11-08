

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(ACCOUNTABILITY_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ACCOUNTABILITY_CODE, 
     last_value(PAYMENT_FUND_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PAYMENT_FUND_TYPE_CODE, 
     last_value(COVERAGE_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVERAGE_TYPE_CODE, 
     last_value(BILL_TYPE_F2_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BILL_TYPE_F2_CODE, 
     last_value(BILL_TYPE_L3_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BILL_TYPE_L3_CODE, 
     last_value(ACCIDENT_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ACCIDENT_TYPE_CODE, 
     last_value(PAYMENT_STATUS_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PAYMENT_STATUS_CODE, 
     last_value(ACCOUNTABILITY_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ACCOUNTABILITY_DESC, 
     last_value(PAYMENT_FUND_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PAYMENT_FUND_DESC, 
     last_value(COVERAGE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as COVERAGE_DESC, 
     last_value(BILL_TYPE_F2_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BILL_TYPE_F2_DESC, 
     last_value(BILL_TYPE_L3_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BILL_TYPE_L3_DESC, 
     last_value(ACCIDENT_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ACCIDENT_TYPE_DESC, 
     last_value(WRQ_STS_CODE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PAYMENT_STATUS_DESC
   
	FROM EDW_STAGING.DIM_PAYMENT_CODER_SCDALL_STEP2),

ETL AS (
select UNIQUE_ID_KEY AS PAYMENT_CODER_HKEY 
    , * 
    , CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'CAM' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
)

SELECT * FROM ETL