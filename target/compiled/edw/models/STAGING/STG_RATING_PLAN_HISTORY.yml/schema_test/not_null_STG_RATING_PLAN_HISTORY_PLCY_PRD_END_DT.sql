
    
    



select count(*) as validation_errors
from STAGING.STG_RATING_PLAN_HISTORY
where PLCY_PRD_END_DT is null


