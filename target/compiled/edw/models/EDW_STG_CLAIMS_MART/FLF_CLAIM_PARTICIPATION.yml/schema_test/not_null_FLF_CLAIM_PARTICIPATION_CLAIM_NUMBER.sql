
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_PARTICIPATION
where CLAIM_NUMBER is null

