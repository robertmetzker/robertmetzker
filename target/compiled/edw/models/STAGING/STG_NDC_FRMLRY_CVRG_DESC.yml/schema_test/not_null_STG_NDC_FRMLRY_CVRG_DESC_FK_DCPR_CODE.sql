
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_FRMLRY_CVRG_DESC
where FK_DCPR_CODE is null


