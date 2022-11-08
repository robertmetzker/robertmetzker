
    
    



select count(*) as validation_errors
from STAGING.DSV_INDIVIDUAL_COVERAGE_TYPE
where INDIVIDUAL_COVERAGE_TYPE_CODE is null


