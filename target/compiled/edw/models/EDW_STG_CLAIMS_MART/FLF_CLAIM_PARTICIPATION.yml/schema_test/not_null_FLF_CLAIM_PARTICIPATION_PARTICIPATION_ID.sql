
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FLF_CLAIM_PARTICIPATION
where PARTICIPATION_ID is null


