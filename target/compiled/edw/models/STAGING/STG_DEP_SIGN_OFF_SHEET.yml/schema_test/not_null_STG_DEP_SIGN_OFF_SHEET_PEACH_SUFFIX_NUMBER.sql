
    
    



select count(*) as validation_errors
from STAGING.STG_DEP_SIGN_OFF_SHEET
where PEACH_SUFFIX_NUMBER is null


