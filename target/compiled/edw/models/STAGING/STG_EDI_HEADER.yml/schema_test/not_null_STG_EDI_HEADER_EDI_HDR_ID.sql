
    
    



select count(*) as validation_errors
from STAGING.STG_EDI_HEADER
where EDI_HDR_ID is null


