
    
    



select count(*) as validation_errors
from (

    select
        PLCY_PRD_ID ||PSH_HIST_EFF_DTM||PSH_HIST_END_DTM|| PSR_HIST_EFF_DTM||PSR_HIST_END_DTM

    from STAGING.STG_POLICY_STATUS_REASON_HISTORY
    where PLCY_PRD_ID ||PSH_HIST_EFF_DTM||PSH_HIST_END_DTM|| PSR_HIST_EFF_DTM||PSR_HIST_END_DTM is not null
    group by PLCY_PRD_ID ||PSH_HIST_EFF_DTM||PSH_HIST_END_DTM|| PSR_HIST_EFF_DTM||PSR_HIST_END_DTM
    having count(*) > 1

) validation_errors


