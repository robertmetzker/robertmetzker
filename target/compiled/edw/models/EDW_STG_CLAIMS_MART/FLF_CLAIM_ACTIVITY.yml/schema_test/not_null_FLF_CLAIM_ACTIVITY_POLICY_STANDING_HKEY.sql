
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_ACTIVITY
where POLICY_STANDING_HKEY is null


