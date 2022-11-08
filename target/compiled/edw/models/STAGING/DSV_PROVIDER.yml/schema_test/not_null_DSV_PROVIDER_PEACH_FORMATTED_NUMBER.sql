
    
    



select count(*) as validation_errors
from STAGING.DSV_PROVIDER
where PEACH_FORMATTED_NUMBER is null


