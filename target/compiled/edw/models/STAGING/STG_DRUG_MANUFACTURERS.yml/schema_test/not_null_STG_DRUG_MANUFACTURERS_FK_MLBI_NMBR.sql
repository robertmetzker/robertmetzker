
    
    



select count(*) as validation_errors
from STAGING.STG_DRUG_MANUFACTURERS
where FK_MLBI_NMBR is null


