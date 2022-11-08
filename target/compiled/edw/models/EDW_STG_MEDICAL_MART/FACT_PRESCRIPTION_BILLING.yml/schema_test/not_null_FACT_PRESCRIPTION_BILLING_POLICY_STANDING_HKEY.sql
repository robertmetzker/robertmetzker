
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PRESCRIPTION_BILLING
where POLICY_STANDING_HKEY is null


