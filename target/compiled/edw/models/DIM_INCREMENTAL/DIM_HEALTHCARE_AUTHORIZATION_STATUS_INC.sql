-- depends_on: EDW_STAGING_DIM.DIM_HEALTHCARE_AUTHORIZATION_STATUS











WITH diff as (
select
    AUTHORIZATION_SERVICE_TYPE_CODE, 
    AUTHORIZATION_SERVICE_TYPE_DESC, 
    AUTHORIZATION_STATUS_CODE, 
    AUTHORIZATION_STATUS_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from  EDW_STAGING_DIM.DIM_HEALTHCARE_AUTHORIZATION_STATUS 
minus
select 
    AUTHORIZATION_SERVICE_TYPE_CODE, 
    AUTHORIZATION_SERVICE_TYPE_DESC, 
    AUTHORIZATION_STATUS_CODE, 
    AUTHORIZATION_STATUS_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from DEV_EDW_32600145.DIM_INCREMENTAL.DIM_HEALTHCARE_AUTHORIZATION_STATUS_INC 
),
src as ( select * from EDW_STAGING_DIM.DIM_HEALTHCARE_AUTHORIZATION_STATUS )
select src.* from src
inner join diff on ( diff.UNIQUE_ID_KEY = src.UNIQUE_ID_KEY )