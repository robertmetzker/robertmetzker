
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE
where UNIQUE_ID_KEY is null


