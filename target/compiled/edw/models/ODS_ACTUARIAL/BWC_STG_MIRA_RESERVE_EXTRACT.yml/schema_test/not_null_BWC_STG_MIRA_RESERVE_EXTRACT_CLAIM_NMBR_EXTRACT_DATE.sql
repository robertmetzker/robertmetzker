
    
    



select count(*) as validation_errors
from ODS_ACTUARIAL.BWC_STG_MIRA_RESERVE_EXTRACT
where CLAIM_NMBR||EXTRACT_DATE is null


