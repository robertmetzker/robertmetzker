




with meet_condition as (

    select * from STAGING.STG_WC_CLASS_SIC_XREF where 1=1

),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(length(WC_CLS_SIC_XREF_CLS_CD) = 4)

)

select count(*)
from validation_errors

