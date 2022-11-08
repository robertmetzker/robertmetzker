
    
    



select count(*) as validation_errors
from STAGING.DST_NDC_GPI_REFERENCE
where UNIQUE_ID_KEY is null


