
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_MANUFACTURERS
where FK_MLBI_NMBR is null


