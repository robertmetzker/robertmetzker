
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_POLICY_ASSIGNMENT
where CLAIM_NUMBER||RELATIONSHIP_END_DATE_KEY is null


