
    
    



select count(*) as validation_errors
from STAGING.DSV_PRESCRIPTION_BILL
where UNIQUE_ID_KEY is null


