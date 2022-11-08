
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_MANUFACTURERS
where MINV_CRT_DTTM is null


