

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_DATE  as
      (

 WITH  SCD AS ( 
	SELECT  DATE_KEY,
     last_value(ACTUAL_DATE ) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ACTUAL_DATE , 
     last_value(DATE_DESC) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DATE_DESC, 
     last_value(HOLIDAY_IND) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as HOLIDAY_IND, 
     last_value(WEEKEND_IND) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as WEEKEND_IND, 
     last_value(WEEK_ENDING_IND) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as WEEK_ENDING_IND, 
     last_value(MONTH_ENDING_IND) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MONTH_ENDING_IND, 
     last_value(YEAR_MONTH_NUMBER) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as YEAR_MONTH_NUMBER, 
     last_value(MONTH_NUMBER) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MONTH_NUMBER, 
     last_value(MONTH_NUMBER_STRING) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MONTH_NUMBER_STRING, 
     last_value(MONTH_NAME) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MONTH_NAME, 
     last_value(MONTH_SHORT_NAME) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MONTH_SHORT_NAME, 
     last_value(MONTH_BEGIN_DATE) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MONTH_BEGIN_DATE, 
     last_value(MONTH_FIRST_BUSINESS_DATE) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MONTH_FIRST_BUSINESS_DATE, 
     last_value(MONTH_END_DATE) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MONTH_END_DATE, 
     last_value(MONTH_LAST_BUSINESS_DATE) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MONTH_LAST_BUSINESS_DATE, 
     last_value(QUARTER_NUMBER) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as QUARTER_NUMBER, 
     last_value(QUARTER_NAME) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as QUARTER_NAME, 
     last_value(HALF_YEAR) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as HALF_YEAR, 
     last_value(YEAR_NUMBER) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as YEAR_NUMBER, 
     last_value(FISCAL_YEAR_BEGIN_DATE) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FISCAL_YEAR_BEGIN_DATE, 
     last_value(FISCAL_YEAR_END_DATE) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FISCAL_YEAR_END_DATE, 
     last_value(PREVIOUS_FISCAL_YEAR_BEGIN_DATE) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PREVIOUS_FISCAL_YEAR_BEGIN_DATE, 
     last_value(PREVIOUS_FISCAL_YEAR_END_DATE) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PREVIOUS_FISCAL_YEAR_END_DATE, 
     last_value(FISCAL_YYYY_NUMBER) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FISCAL_YYYY_NUMBER, 
     last_value(FISCAL_QUARTER_NAME) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FISCAL_QUARTER_NAME, 
     last_value(FISCAL_QUARTER_NUMBER) over 
        (partition by DATE_KEY 
        order by DATE_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FISCAL_QUARTER_NUMBER

	FROM EDW_STAGING.DIM_DATE_SCDALL_STEP2),

---ETL LAYER----------
ETL AS (
select 
DATE_KEY,
ACTUAL_DATE, 
DATE_DESC,
HOLIDAY_IND,
WEEKEND_IND,
WEEK_ENDING_IND,
MONTH_ENDING_IND,
YEAR_MONTH_NUMBER,
MONTH_NUMBER:: NUMERIC(12,0) AS MONTH_NUMBER,
MONTH_NUMBER_STRING,
MONTH_NAME,
MONTH_SHORT_NAME,
MONTH_BEGIN_DATE,
MONTH_FIRST_BUSINESS_DATE,
MONTH_END_DATE,
MONTH_LAST_BUSINESS_DATE,
QUARTER_NUMBER,
QUARTER_NAME,
HALF_YEAR,
YEAR_NUMBER,
FISCAL_YEAR_BEGIN_DATE,
FISCAL_YEAR_END_DATE,
PREVIOUS_FISCAL_YEAR_BEGIN_DATE,
PREVIOUS_FISCAL_YEAR_END_DATE,
FISCAL_YYYY_NUMBER,
FISCAL_QUARTER_NAME,
FISCAL_QUARTER_NUMBER,
CURRENT_TIMESTAMP AS  LOAD_DATETIME,
TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME,
'DW_REPORT' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
)

select * from ETL
      );
    