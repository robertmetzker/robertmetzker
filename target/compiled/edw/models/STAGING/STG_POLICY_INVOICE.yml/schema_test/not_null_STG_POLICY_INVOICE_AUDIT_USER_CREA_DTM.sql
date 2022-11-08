
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_INVOICE
where AUDIT_USER_CREA_DTM is null


