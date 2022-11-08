
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_INJURY_HISTORY_FRZN
where HIST_ID is null


