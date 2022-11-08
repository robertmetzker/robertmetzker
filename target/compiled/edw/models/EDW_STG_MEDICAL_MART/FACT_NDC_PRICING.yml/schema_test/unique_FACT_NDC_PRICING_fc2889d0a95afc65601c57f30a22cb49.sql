
    
    



select count(*) as validation_errors
from (

    select
        NDC_GPI_HKEY||'-'||PRICING_TYPE_HKEY||'-'||EFFECTIVE_DATE_KEY||'-'||END_DATE_KEY

    from EDW_STG_MEDICAL_MART.FACT_NDC_PRICING
    where NDC_GPI_HKEY||'-'||PRICING_TYPE_HKEY||'-'||EFFECTIVE_DATE_KEY||'-'||END_DATE_KEY is not null
    group by NDC_GPI_HKEY||'-'||PRICING_TYPE_HKEY||'-'||EFFECTIVE_DATE_KEY||'-'||END_DATE_KEY
    having count(*) > 1

) validation_errors


