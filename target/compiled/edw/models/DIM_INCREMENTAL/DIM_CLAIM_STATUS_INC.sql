-- depends_on: EDW_STAGING_DIM.DIM_CLAIM_STATUS











WITH diff as (
select
    CLAIM_STATE_CODE, 
    CLAIM_STATE_DESC, 
    CLAIM_STATUS_CODE, 
    CLAIM_STATUS_DESC, 
    CLAIM_STATUS_REASON_CODE, 
    CLAIM_STATUS_REASON_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from  EDW_STAGING_DIM.DIM_CLAIM_STATUS 
minus
select 
    CLAIM_STATE_CODE, 
    CLAIM_STATE_DESC, 
    CLAIM_STATUS_CODE, 
    CLAIM_STATUS_DESC, 
    CLAIM_STATUS_REASON_CODE, 
    CLAIM_STATUS_REASON_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from DEV_EDW_32600145.DIM_INCREMENTAL.DIM_CLAIM_STATUS_INC 
),
src as ( select * from EDW_STAGING_DIM.DIM_CLAIM_STATUS )
select src.* from src
inner join diff on ( diff.UNIQUE_ID_KEY = src.UNIQUE_ID_KEY )