
    
    



select count(*) as validation_errors
from (

    select
        INDIVIDUAL_COVERAGE_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_INDIVIDUAL_COVERAGE_TYPE
    where INDIVIDUAL_COVERAGE_TYPE_HKEY is not null
    group by INDIVIDUAL_COVERAGE_TYPE_HKEY
    having count(*) > 1

) validation_errors


