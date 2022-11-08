
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_HOSPITAL_OUTPATIENT_BILLING
where MODIFIER_SEQUENCE_HKEY is null


