
    
    



select count(*) as validation_errors
from STAGING.STG_NOTE_APPLICATION
where NOTE_APP_DTL_LVL_ID is null


