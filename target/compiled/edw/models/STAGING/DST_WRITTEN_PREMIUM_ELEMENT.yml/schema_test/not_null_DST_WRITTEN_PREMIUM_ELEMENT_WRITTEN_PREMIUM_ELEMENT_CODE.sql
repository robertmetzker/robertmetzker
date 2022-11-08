
    
    



select count(*) as validation_errors
from STAGING.DST_WRITTEN_PREMIUM_ELEMENT
where WRITTEN_PREMIUM_ELEMENT_CODE is null


