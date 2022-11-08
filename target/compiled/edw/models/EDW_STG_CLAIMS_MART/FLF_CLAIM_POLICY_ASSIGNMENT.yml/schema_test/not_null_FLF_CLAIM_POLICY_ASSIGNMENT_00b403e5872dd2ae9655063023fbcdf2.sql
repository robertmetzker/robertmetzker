
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_POLICY_ASSIGNMENT
where CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY is null


