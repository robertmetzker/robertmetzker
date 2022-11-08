-- depends_on: EDW_STAGING_DIM.DIM_ICD_ADMISSION_PRESENCE











WITH diff as (
select
    PRESENT_ON_ADMISSION_CODE, 
    PRESENT_ON_ADMISSION_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from  EDW_STAGING_DIM.DIM_ICD_ADMISSION_PRESENCE 
minus
select 
    PRESENT_ON_ADMISSION_CODE, 
    PRESENT_ON_ADMISSION_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from DEV_EDW_32600145.DIM_INCREMENTAL.DIM_ICD_ADMISSION_PRESENCE_INC 
),
src as ( select * from EDW_STAGING_DIM.DIM_ICD_ADMISSION_PRESENCE )
select src.* from src
inner join diff on ( diff.UNIQUE_ID_KEY = src.UNIQUE_ID_KEY )