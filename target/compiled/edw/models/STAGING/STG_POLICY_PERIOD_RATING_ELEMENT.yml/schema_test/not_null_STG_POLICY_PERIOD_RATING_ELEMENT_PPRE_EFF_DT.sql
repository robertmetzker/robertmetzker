
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_PERIOD_RATING_ELEMENT
where PPRE_EFF_DT is null


