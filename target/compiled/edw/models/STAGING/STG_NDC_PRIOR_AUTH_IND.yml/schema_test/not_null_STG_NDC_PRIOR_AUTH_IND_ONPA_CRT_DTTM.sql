
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_PRIOR_AUTH_IND
where ONPA_CRT_DTTM is null


