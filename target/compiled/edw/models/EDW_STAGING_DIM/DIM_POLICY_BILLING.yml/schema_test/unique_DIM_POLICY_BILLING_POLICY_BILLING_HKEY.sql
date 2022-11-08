
    
    



select count(*) as validation_errors
from (

    select
        POLICY_BILLING_HKEY

    from EDW_STAGING_DIM.DIM_POLICY_BILLING
    where POLICY_BILLING_HKEY is not null
    group by POLICY_BILLING_HKEY
    having count(*) > 1

) validation_errors


