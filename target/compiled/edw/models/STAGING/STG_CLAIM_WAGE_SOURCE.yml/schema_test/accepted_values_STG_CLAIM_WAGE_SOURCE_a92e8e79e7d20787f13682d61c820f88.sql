
    
    




with all_values as (

    select distinct
        CLM_EMPL_STS_TYP_NM as value_field

    from STAGING.STG_CLAIM_WAGE_SOURCE

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'FULL TIME','INJURED DURING REHAB','NOT EMPLOYED','OTHER','PART TIME','PIECEWORK','RETIRED','SEASONAL','SELF EMPLOYED','VOLUNTEER'
    )
)

select count(*) as validation_errors
from validation_errors


