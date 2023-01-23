-- depends_on: EDW_STAGING_DIM.DIM_INVOICE_STATUS











WITH diff as (
select
    MEDICAL_INVOICE_STATUS_CODE, 
    MEDICAL_INVOICE_STATUS_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from  EDW_STAGING_DIM.DIM_INVOICE_STATUS 
minus
select 
    MEDICAL_INVOICE_STATUS_CODE, 
    MEDICAL_INVOICE_STATUS_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from DEV_EDW_32600145.DIM_INCREMENTAL.DIM_INVOICE_STATUS_INC 
),
src as ( select * from EDW_STAGING_DIM.DIM_INVOICE_STATUS )
select src.* from src
inner join diff on ( diff.UNIQUE_ID_KEY = src.UNIQUE_ID_KEY )