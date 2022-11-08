
    
    



select count(*) as validation_errors
from STAGING.STG_INDEMNITY_PAYMENT
where AUDIT_USER_CREA_DTM is null


