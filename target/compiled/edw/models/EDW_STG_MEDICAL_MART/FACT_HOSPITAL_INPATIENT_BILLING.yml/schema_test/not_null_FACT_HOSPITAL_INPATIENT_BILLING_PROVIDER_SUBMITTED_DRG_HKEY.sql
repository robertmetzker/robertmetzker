
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_HOSPITAL_INPATIENT_BILLING
where PROVIDER_SUBMITTED_DRG_HKEY is null


