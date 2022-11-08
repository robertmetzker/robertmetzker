

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_INVOICE_PROFILE  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     first_value(MEDICAL_INVOICE_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MEDICAL_INVOICE_TYPE_CODE, 
     first_value(INVOICE_FEE_SCHEDULE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INVOICE_FEE_SCHEDULE_DESC, 
     first_value(INVOICE_PAYMENT_CATEGORY) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INVOICE_PAYMENT_CATEGORY, 
     first_value(ADJUSTMENT_TYPE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ADJUSTMENT_TYPE, 
     first_value(PAID_ABOVE_ZERO_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PAID_ABOVE_ZERO_IND, 
     first_value(IN_SUBROGATION_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as IN_SUBROGATION_IND, 
     first_value(SUBROGATION_TYPE_DESC ) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUBROGATION_TYPE_DESC , 
     first_value(INVOICE_INPUT_METHOD_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INVOICE_INPUT_METHOD_CODE, 
     first_value(INVOICE_INPUT_METHOD_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INVOICE_INPUT_METHOD_DESC
	FROM EDW_STAGING.DIM_INVOICE_PROFILE_SCDALL_STEP2)

select 
md5(cast(
    
    coalesce(cast(MEDICAL_INVOICE_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(IN_SUBROGATION_IND as 
    varchar
), '') || '-' || coalesce(cast(ADJUSTMENT_TYPE as 
    varchar
), '') || '-' || coalesce(cast(INVOICE_INPUT_METHOD_CODE as 
    varchar
), '') || '-' || coalesce(cast(INVOICE_PAYMENT_CATEGORY as 
    varchar
), '') || '-' || coalesce(cast(INVOICE_FEE_SCHEDULE_DESC as 
    varchar
), '') || '-' || coalesce(cast(PAID_ABOVE_ZERO_IND as 
    varchar
), '') || '-' || coalesce(cast(SUBROGATION_TYPE_DESC as 
    varchar
), '')

 as 
    varchar
))
AS INVOICE_PROFILE_HKEY
, UNIQUE_ID_KEY
, MEDICAL_INVOICE_TYPE_CODE
, INVOICE_FEE_SCHEDULE_DESC
, INVOICE_PAYMENT_CATEGORY
, ADJUSTMENT_TYPE
, PAID_ABOVE_ZERO_IND
, IN_SUBROGATION_IND
, SUBROGATION_TYPE_DESC 
, INVOICE_INPUT_METHOD_CODE
, INVOICE_INPUT_METHOD_DESC
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, try_to_TIMESTAMP('Invalid')  AS UPDATE_DATETIME
, 'CAM' AS PRIMARY_SOURCE_SYSTEM
 from SCD
      );
    