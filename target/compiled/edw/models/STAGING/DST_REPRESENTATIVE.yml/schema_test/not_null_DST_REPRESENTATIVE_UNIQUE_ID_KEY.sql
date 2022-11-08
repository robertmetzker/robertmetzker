
    
    



select count(*) as validation_errors
from STAGING.DST_REPRESENTATIVE
where UNIQUE_ID_KEY is null


