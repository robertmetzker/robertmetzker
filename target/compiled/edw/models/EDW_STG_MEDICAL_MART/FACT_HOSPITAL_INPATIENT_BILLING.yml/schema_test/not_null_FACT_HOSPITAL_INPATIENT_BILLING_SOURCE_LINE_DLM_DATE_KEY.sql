
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_HOSPITAL_INPATIENT_BILLING
where SOURCE_LINE_DLM_DATE_KEY is null


