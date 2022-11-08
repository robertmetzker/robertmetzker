
    
    



select count(*) as validation_errors
from STAGING.DSV_MODIFIER_SEQUENCE
where UNIQUE_ID_KEY is null


