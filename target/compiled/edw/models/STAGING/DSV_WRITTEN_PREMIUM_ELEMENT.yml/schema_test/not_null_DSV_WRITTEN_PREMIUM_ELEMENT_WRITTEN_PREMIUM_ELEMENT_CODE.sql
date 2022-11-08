
    
    



select count(*) as validation_errors
from STAGING.DSV_WRITTEN_PREMIUM_ELEMENT
where WRITTEN_PREMIUM_ELEMENT_CODE is null


