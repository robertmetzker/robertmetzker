-- depends_on: EDW_STAGING_DIM.DIM_PAYEE











WITH diff as (
select
    PAYEE_FULL_NAME, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from  EDW_STAGING_DIM.DIM_PAYEE 
minus
select 
    PAYEE_FULL_NAME, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from DEV_EDW_32600145.DIM_INCREMENTAL.DIM_PAYEE_INC 
),
src as ( select * from EDW_STAGING_DIM.DIM_PAYEE )
select src.* from src
inner join diff on ( diff.UNIQUE_ID_KEY = src.UNIQUE_ID_KEY )