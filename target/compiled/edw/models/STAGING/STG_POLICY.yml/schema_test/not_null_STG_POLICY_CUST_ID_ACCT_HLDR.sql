
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY
where CUST_ID_ACCT_HLDR is null


