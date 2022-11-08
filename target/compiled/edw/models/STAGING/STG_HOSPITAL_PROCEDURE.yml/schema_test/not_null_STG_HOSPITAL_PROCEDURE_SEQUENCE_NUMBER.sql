
    
    



select count(*) as validation_errors
from STAGING.STG_HOSPITAL_PROCEDURE
where SEQUENCE_NUMBER is null


