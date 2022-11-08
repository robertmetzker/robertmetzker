
    
    




with all_values as (

    select distinct
        TAX_ID_TYP_CD as value_field

    from STAGING.STG_TAX_IDENTIFIER

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        'BN','DUP_SSN','EVN','FEIN','GCN','PN','SIN','SSN'
    )
)

select count(*) as validation_errors
from validation_errors


