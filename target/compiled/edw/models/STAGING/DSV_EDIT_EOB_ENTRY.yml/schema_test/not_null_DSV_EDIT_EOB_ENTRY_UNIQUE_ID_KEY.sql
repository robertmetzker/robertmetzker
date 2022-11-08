
    
    



select count(*) as validation_errors
from STAGING.DSV_EDIT_EOB_ENTRY
where UNIQUE_ID_KEY is null


