
    
    



select count(*) as validation_errors
from STAGING.DST_POLICY_BILLING
where POLICY_15K_PROGRAM_IND is null


