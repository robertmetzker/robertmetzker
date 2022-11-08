

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_POLICY_PERIOD  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(POLICY_PERIOD_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as POLICY_PERIOD_DESC, 
     last_value(POLICY_PERIOD_EFFECTIVE_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as POLICY_PERIOD_EFFECTIVE_DATE, 
     last_value(POLICY_PERIOD_END_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as POLICY_PERIOD_END_DATE,    
     last_value(PEC_POLICY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PEC_POLICY_IND,
     last_value(NEW_POLICY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NEW_POLICY_IND,
     last_value(REPORTING_YEAR_EFFECTIVE_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as REPORTING_YEAR_EFFECTIVE_DATE, 
     last_value(REPORTING_YEAR_END_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as REPORTING_YEAR_END_DATE, 
     last_value(REPORTING_YEAR_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as REPORTING_YEAR_DESC 

     
	FROM EDW_STAGING.DIM_POLICY_PERIOD_SCDALL_STEP2),


ETL AS (
select UNIQUE_ID_KEY AS POLICY_PERIOD_HKEY 
    , * 
    , CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
)

SELECT * FROM ETL
      );
    