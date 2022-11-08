
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_CONSOLIDATED_MEDICAL_BILLING
where FINANCIALLY_RESPONSIBLE_NETWORK_HKEY is null


