
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_HOSPITAL_OUTPATIENT_BILLING
where PRIMARY_SOURCE_SYSTEM is null


