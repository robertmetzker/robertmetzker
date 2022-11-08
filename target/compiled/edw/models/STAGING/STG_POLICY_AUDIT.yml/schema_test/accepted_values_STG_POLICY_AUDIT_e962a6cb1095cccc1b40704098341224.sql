
    
    




with all_values as (

    select distinct
        PAUAST_NM as value_field

    from STAGING.STG_POLICY_AUDIT

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'APPROVED','COMPLETED','PENDING UW REVIEW','REJECTED'
    )
)

select count(*) as validation_errors
from validation_errors


