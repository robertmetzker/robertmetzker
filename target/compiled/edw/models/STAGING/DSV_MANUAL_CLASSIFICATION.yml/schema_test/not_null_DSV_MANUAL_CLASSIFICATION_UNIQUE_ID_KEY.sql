
    
    



select count(*) as validation_errors
from STAGING.DSV_MANUAL_CLASSIFICATION
where UNIQUE_ID_KEY is null


