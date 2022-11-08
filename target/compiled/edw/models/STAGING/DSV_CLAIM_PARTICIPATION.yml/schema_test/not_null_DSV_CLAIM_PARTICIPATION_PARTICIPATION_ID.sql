
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PARTICIPATION
where PARTICIPATION_ID is null


