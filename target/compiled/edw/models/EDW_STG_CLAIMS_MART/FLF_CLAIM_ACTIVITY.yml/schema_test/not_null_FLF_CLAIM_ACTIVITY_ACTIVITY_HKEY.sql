
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_ACTIVITY
where ACTIVITY_HKEY is null


