
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_CONSOLIDATED_MEDICAL_BILLING
where MODIFIER_SEQUENCE_HKEY is null

