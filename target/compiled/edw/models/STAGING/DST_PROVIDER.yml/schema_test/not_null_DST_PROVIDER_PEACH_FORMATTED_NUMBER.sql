
    
    



select count(*) as validation_errors
from STAGING.DST_PROVIDER
where PEACH_FORMATTED_NUMBER is null


