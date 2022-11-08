
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_DETAIL_FILE_REVIEW
where CDFR_ID is null


