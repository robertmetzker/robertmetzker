
    
    



select count(*) as validation_errors
from STAGING.DST_OUTPATIENT_GROUPER
where UNIQUE_ID_KEY is null


