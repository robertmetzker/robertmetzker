-- depends_on: EDW_STAGING_DIM.DIM_DIAGNOSIS_RELATED_GROUP











WITH diff as (
select
    CURRENT_RECORD_IND, 
    DIAGNOSIS_RELATED_GROUP_CODE, 
    DIAGNOSIS_RELATED_GROUP_TITLE, 
    DRG_EFFECTIVE_DATE, 
    DRG_EXPIRATION_DATE, 
    DRG_TYPE_DESC, 
    PA_TYPE_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    REVIEW_IND, 
    UNIQUE_ID_KEY
    from  EDW_STAGING_DIM.DIM_DIAGNOSIS_RELATED_GROUP 
where current_record_ind = 'Y'
minus
select 
    CURRENT_RECORD_IND, 
    DIAGNOSIS_RELATED_GROUP_CODE, 
    DIAGNOSIS_RELATED_GROUP_TITLE, 
    DRG_EFFECTIVE_DATE, 
    DRG_EXPIRATION_DATE, 
    DRG_TYPE_DESC, 
    PA_TYPE_DESC, 
    PRIMARY_SOURCE_SYSTEM, 
    REVIEW_IND, 
    UNIQUE_ID_KEY
    from DEV_EDW_32600145.DIM_INCREMENTAL.DIM_DIAGNOSIS_RELATED_GROUP_INC 
where current_record_ind = 'Y'
),
src as ( select * from EDW_STAGING_DIM.DIM_DIAGNOSIS_RELATED_GROUP )
select src.* from src
inner join diff on ( diff.UNIQUE_ID_KEY = src.UNIQUE_ID_KEY )