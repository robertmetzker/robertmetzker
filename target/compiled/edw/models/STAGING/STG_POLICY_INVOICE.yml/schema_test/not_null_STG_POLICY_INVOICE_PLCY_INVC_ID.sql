
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_INVOICE
where PLCY_INVC_ID is null


