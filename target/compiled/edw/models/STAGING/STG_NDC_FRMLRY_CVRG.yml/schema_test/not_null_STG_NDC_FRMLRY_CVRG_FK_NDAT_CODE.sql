
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_FRMLRY_CVRG
where FK_NDAT_CODE is null

