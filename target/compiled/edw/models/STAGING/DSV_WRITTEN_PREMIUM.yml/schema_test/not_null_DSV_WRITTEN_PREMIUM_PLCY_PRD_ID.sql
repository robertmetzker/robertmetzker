
    
    



select count(*) as validation_errors
from STAGING.DSV_WRITTEN_PREMIUM
where PLCY_PRD_ID is null


