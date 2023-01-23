-- depends_on: EDW_STAGING_DIM.DIM_CPT











WITH diff as (
select
    CPT_FEE_SCHEDULE_DESC, 
    CPT_PAYMENT_CATEGORY, 
    CPT_PAYMENT_SUBCATEGORY, 
    CURRENT_RECORD_IND, 
    PRIMARY_SOURCE_SYSTEM, 
    PROCEDURE_CODE, 
    PROCEDURE_CODE_EFFECTIVE_DATE, 
    PROCEDURE_CODE_END_DATE, 
    PROCEDURE_CODE_ENTRY_DATE, 
    PROCEDURE_DESC, 
    PROCEDURE_SERVICE_TYPE_DESC, 
    UNIQUE_ID_KEY
    from  EDW_STAGING_DIM.DIM_CPT 
where current_record_ind = 'Y'
minus
select 
    CPT_FEE_SCHEDULE_DESC, 
    CPT_PAYMENT_CATEGORY, 
    CPT_PAYMENT_SUBCATEGORY, 
    CURRENT_RECORD_IND, 
    PRIMARY_SOURCE_SYSTEM, 
    PROCEDURE_CODE, 
    PROCEDURE_CODE_EFFECTIVE_DATE, 
    PROCEDURE_CODE_END_DATE, 
    PROCEDURE_CODE_ENTRY_DATE, 
    PROCEDURE_DESC, 
    PROCEDURE_SERVICE_TYPE_DESC, 
    UNIQUE_ID_KEY
    from DEV_EDW_32600145.DIM_INCREMENTAL.DIM_CPT_INC 
where current_record_ind = 'Y'
),
src as ( select * from EDW_STAGING_DIM.DIM_CPT )
select src.* from src
inner join diff on ( diff.UNIQUE_ID_KEY = src.UNIQUE_ID_KEY )