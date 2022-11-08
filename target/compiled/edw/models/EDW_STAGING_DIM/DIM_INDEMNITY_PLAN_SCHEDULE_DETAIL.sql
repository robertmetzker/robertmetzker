

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(FREQUENCY_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FREQUENCY_TYPE_CODE, 
     last_value(INDEMNITY_REASON_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INDEMNITY_REASON_TYPE_CODE, 
     last_value(SCHEDULE_DETAIL_AMOUNT_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_DETAIL_AMOUNT_TYPE_CODE, 
     last_value(SCHEDULE_PAYMENT_DETAIL_STATUS_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_PAYMENT_DETAIL_STATUS_TYPE_CODE, 
     last_value(FINAL_PAYMENT_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FINAL_PAYMENT_IND, 
     last_value(SCHEDULE_AUTO_PAY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_AUTO_PAY_IND, 
     last_value(INDEMNITY_RECALCULATION_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INDEMNITY_RECALCULATION_IND, 
     last_value(PRIMARY_PAYEE_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRIMARY_PAYEE_IND, 
     last_value(PAYMENT_MAILTO_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PAYMENT_MAILTO_IND, 
     last_value(SCHEDULE_DETAIL_AMOUNT_REMAINING_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_DETAIL_AMOUNT_REMAINING_IND, 
     last_value(OVERPAYMENT_BALANCE_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OVERPAYMENT_BALANCE_IND, 
     last_value(PLAN_VOID_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PLAN_VOID_IND, 
     last_value(SCHEDULE_VOID_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_VOID_IND, 
     last_value(SCHEDULE_DETAIL_VOID_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_DETAIL_VOID_IND, 
     last_value(SCHEDULE_PAYMENT_DETAIL_VOID_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_PAYMENT_DETAIL_VOID_IND, 
     last_value(FREQUENCY_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FREQUENCY_TYPE_DESC, 
     last_value(INDEMNITY_REASON_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INDEMNITY_REASON_TYPE_DESC, 
     last_value(SCHEDULE_PAYMENT_DETAIL_STATUS_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_PAYMENT_DETAIL_STATUS_TYPE_DESC, 
     last_value(SCHEDULE_DETAIL_AMOUNT_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_DETAIL_AMOUNT_TYPE_DESC

	FROM EDW_STAGING.DIM_INDEMNITY_PLAN_SCHEDULE_DETAIL_SCDALL_STEP2)

---- ETL LAYER ----
, ETL AS (select 
  md5(cast(
    
    coalesce(cast(FREQUENCY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(INDEMNITY_REASON_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(SCHEDULE_DETAIL_AMOUNT_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(SCHEDULE_PAYMENT_DETAIL_STATUS_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(FINAL_PAYMENT_IND as 
    varchar
), '') || '-' || coalesce(cast(SCHEDULE_AUTO_PAY_IND as 
    varchar
), '') || '-' || coalesce(cast(INDEMNITY_RECALCULATION_IND as 
    varchar
), '') || '-' || coalesce(cast(PRIMARY_PAYEE_IND as 
    varchar
), '') || '-' || coalesce(cast(PAYMENT_MAILTO_IND as 
    varchar
), '') || '-' || coalesce(cast(SCHEDULE_DETAIL_AMOUNT_REMAINING_IND as 
    varchar
), '') || '-' || coalesce(cast(OVERPAYMENT_BALANCE_IND as 
    varchar
), '') || '-' || coalesce(cast(PLAN_VOID_IND as 
    varchar
), '') || '-' || coalesce(cast(SCHEDULE_VOID_IND as 
    varchar
), '') || '-' || coalesce(cast(SCHEDULE_DETAIL_VOID_IND as 
    varchar
), '') || '-' || coalesce(cast(SCHEDULE_PAYMENT_DETAIL_VOID_IND as 
    varchar
), '')

 as 
    varchar
)) as INDEMNITY_PLAN_SCHEDULE_DETAIL_HKEY
    , UNIQUE_ID_KEY
    , FREQUENCY_TYPE_CODE
    , INDEMNITY_REASON_TYPE_CODE
    , SCHEDULE_DETAIL_AMOUNT_TYPE_CODE
    , SCHEDULE_PAYMENT_DETAIL_STATUS_TYPE_CODE
    , FINAL_PAYMENT_IND
    , SCHEDULE_AUTO_PAY_IND
    , INDEMNITY_RECALCULATION_IND
    , PRIMARY_PAYEE_IND
    , PAYMENT_MAILTO_IND
    , SCHEDULE_DETAIL_AMOUNT_REMAINING_IND
    , OVERPAYMENT_BALANCE_IND
    , PLAN_VOID_IND
    , SCHEDULE_VOID_IND
    , SCHEDULE_DETAIL_VOID_IND
    , SCHEDULE_PAYMENT_DETAIL_VOID_IND
    , FREQUENCY_TYPE_DESC
    , INDEMNITY_REASON_TYPE_DESC
    , SCHEDULE_PAYMENT_DETAIL_STATUS_TYPE_DESC
    , SCHEDULE_DETAIL_AMOUNT_TYPE_DESC
	, CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
 from SCD )

 SELECT * FROM ETL