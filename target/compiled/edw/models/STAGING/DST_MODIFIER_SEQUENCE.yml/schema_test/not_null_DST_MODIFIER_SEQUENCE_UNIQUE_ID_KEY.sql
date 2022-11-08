
    
    



select count(*) as validation_errors
from STAGING.DST_MODIFIER_SEQUENCE
where UNIQUE_ID_KEY is null


