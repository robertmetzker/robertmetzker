
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_HOSPITAL_INPATIENT_BILLING
where CLAIM_TYPE_STATUS_HKEY is null


