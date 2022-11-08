
    
    



select count(*) as validation_errors
from STAGING.STG_DEP_SIGN_OFF_SHEET
where PEACH_BASE_NUMBER is null


