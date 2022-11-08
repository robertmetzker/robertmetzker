
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_AVERAGE_WAGE_SOURCE_DETAIL
where CAWWSD_ID is null


