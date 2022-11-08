
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_CODES
where FK_MDCT_CODE is null


