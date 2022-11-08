
    
    



select count(*) as validation_errors
from (

    select
        CLM_AGRE_ID||CLM_PLCY_RLTNS_EFF_DATE

    from STAGING.STG_CLAIM_POLICY_HISTORY
    where CLM_AGRE_ID||CLM_PLCY_RLTNS_EFF_DATE is not null
    group by CLM_AGRE_ID||CLM_PLCY_RLTNS_EFF_DATE
    having count(*) > 1

) validation_errors


