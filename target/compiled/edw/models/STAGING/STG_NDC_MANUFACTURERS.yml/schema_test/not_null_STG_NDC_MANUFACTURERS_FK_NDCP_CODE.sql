
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_MANUFACTURERS
where FK_NDCP_CODE is null

