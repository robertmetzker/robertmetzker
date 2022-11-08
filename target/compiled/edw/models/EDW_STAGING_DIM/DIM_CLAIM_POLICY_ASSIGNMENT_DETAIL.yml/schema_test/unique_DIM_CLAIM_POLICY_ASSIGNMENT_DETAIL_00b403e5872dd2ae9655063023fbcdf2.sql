
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY

    from EDW_STAGING_DIM.DIM_CLAIM_POLICY_ASSIGNMENT_DETAIL
    where CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY is not null
    group by CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY
    having count(*) > 1

) validation_errors


