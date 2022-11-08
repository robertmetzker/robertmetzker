
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PRESCRIPTION_BILLING
where SUBMITTING_NETWORK_HKEY is null


