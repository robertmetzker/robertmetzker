
    
    



select count(*) as validation_errors
from STAGING.DSV_REPRESENTATIVE
where UNIQUE_ID_KEY is null


