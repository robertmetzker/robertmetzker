
    
    



select count(*) as validation_errors
from STAGING.STG_PERSON_HISTORY
where HIST_EFF_DTM is null


