
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_INTERACTION_CHANNEL
where AUDIT_USER_CREA_DTM is null


