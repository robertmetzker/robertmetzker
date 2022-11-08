
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_CODES
where MNDC_CRT_DTTM is null


