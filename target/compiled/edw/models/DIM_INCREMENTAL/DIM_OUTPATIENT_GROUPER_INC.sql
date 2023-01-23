-- depends_on: EDW_STAGING_DIM.DIM_OUTPATIENT_GROUPER











WITH diff as (
select
    APC_COMPOSITE_ADJUSTMENT_CODE, 
    APC_COMPOSITE_ADJUSTMENT_DESC, 
    APC_DISCOUNTING_FRACTION_CODE, 
    APC_DISCOUNTING_FRACTION_DESC, 
    APC_PACKAGING_CODE, 
    APC_PACKAGING_DESC, 
    APC_PAYMENT_ADJUSTMENT_CODE, 
    APC_PAYMENT_ADJUSTMENT_DESC, 
    APC_PAYMENT_INDICATOR_CODE, 
    APC_PAYMENT_INDICATOR_DESC, 
    APC_STATUS_INDICATOR_CODE, 
    APC_STATUS_INDICATOR_DESC, 
    OPPS_CODE, 
    OPPS_DESC, 
    OPPS_PAYMENT_METHOD_CODE, 
    OPPS_PAYMENT_METHOD_DESC, 
    OPPS_RETURN_CODE, 
    OPPS_RETURN_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from  EDW_STAGING_DIM.DIM_OUTPATIENT_GROUPER 
minus
select 
    APC_COMPOSITE_ADJUSTMENT_CODE, 
    APC_COMPOSITE_ADJUSTMENT_DESC, 
    APC_DISCOUNTING_FRACTION_CODE, 
    APC_DISCOUNTING_FRACTION_DESC, 
    APC_PACKAGING_CODE, 
    APC_PACKAGING_DESC, 
    APC_PAYMENT_ADJUSTMENT_CODE, 
    APC_PAYMENT_ADJUSTMENT_DESC, 
    APC_PAYMENT_INDICATOR_CODE, 
    APC_PAYMENT_INDICATOR_DESC, 
    APC_STATUS_INDICATOR_CODE, 
    APC_STATUS_INDICATOR_DESC, 
    OPPS_CODE, 
    OPPS_DESC, 
    OPPS_PAYMENT_METHOD_CODE, 
    OPPS_PAYMENT_METHOD_DESC, 
    OPPS_RETURN_CODE, 
    OPPS_RETURN_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    UNIQUE_ID_KEY
    from DEV_EDW_32600145.DIM_INCREMENTAL.DIM_OUTPATIENT_GROUPER_INC 
),
src as ( select * from EDW_STAGING_DIM.DIM_OUTPATIENT_GROUPER )
select src.* from src
inner join diff on ( diff.UNIQUE_ID_KEY = src.UNIQUE_ID_KEY )