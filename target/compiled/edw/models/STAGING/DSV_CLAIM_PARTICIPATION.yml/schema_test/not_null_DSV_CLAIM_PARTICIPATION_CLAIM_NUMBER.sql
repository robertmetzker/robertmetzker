
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PARTICIPATION
where CLAIM_NUMBER is null


