
    
    



select count(*) as validation_errors
from (

    select
        POLICY_NUMBER ||RATING_ELEMENT_HKEY || RATING_ELEMENT_EFFECTIVE_DATE_KEY || RATING_ELEMENT_END_DATE_KEY

    from EDW_STG_POLICY_MART.FACT_POLICY_PERIOD_RATING_ELEMENTS
    where POLICY_NUMBER ||RATING_ELEMENT_HKEY || RATING_ELEMENT_EFFECTIVE_DATE_KEY || RATING_ELEMENT_END_DATE_KEY is not null
    group by POLICY_NUMBER ||RATING_ELEMENT_HKEY || RATING_ELEMENT_EFFECTIVE_DATE_KEY || RATING_ELEMENT_END_DATE_KEY
    having count(*) > 1

) validation_errors

