
    
    



select count(*) as validation_errors
from STAGING.DSV_ADMISSION
where UNIQUE_ID_KEY is null


