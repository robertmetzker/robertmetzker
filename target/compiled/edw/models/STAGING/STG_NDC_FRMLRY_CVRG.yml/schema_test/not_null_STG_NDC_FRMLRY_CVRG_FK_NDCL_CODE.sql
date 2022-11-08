
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_FRMLRY_CVRG
where FK_NDCL_CODE is null


