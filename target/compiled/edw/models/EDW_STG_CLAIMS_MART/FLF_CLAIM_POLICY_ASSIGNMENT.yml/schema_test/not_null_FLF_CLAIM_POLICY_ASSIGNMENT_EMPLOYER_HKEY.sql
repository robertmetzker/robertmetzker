
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_POLICY_ASSIGNMENT
where EMPLOYER_HKEY is null

