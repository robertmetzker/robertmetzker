
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_PERIOD_RATING_ELEMENT
where AUDIT_USER_ID_CREA is null


