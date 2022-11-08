





with validation_errors as (

    select
        BENEFIT_TYPE_CODE, JURISDICTION_TYPE_CODE, BENEFIT_REPORTING_TYPE_DESC, INJURY_TYPE_CODE
    from EDW_STAGING_DIM.DIM_BENEFIT_TYPE

    group by BENEFIT_TYPE_CODE, JURISDICTION_TYPE_CODE, BENEFIT_REPORTING_TYPE_DESC, INJURY_TYPE_CODE
    having count(*) > 1

)

select count(*)
from validation_errors


