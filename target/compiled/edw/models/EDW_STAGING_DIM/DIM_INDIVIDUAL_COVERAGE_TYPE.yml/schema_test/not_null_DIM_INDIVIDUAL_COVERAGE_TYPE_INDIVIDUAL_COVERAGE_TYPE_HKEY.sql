
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INDIVIDUAL_COVERAGE_TYPE
where INDIVIDUAL_COVERAGE_TYPE_HKEY is null


