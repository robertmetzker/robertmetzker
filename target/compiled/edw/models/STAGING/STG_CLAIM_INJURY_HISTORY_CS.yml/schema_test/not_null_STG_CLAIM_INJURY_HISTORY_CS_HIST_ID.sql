
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_INJURY_HISTORY_CS
where HIST_ID is null


