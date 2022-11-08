




with meet_condition as (

    select * from STAGING.STG_NDC_VRSN_DURATION where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(FK_NDCL_CODE) = 5)

)

select count(*)
from validation_errors

