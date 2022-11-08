
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_INVOICE
where AUDIT_USER_ID_CREA is null


