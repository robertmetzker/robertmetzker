
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_OTHER_DATE
where CLM_OTHR_DT_ID is null


