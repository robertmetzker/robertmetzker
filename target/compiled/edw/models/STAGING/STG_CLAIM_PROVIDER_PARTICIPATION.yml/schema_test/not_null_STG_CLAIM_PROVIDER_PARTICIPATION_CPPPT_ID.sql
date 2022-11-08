
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_PROVIDER_PARTICIPATION
where CPPPT_ID is null


