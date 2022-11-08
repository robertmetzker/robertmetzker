
    
    



select count(*) as validation_errors
from STAGING.DSV_DISABILITY_MANAGEMENT
where CLAIM_NUMBER is null


