
    
    



select count(*) as validation_errors
from (

    select
        PAY_MEDA_PREF_TYP_CD || PAY_REQS_TYP_CD || PAY_REQS_STT_TYP_CD || PAY_REQS_STS_TYP_CD || PAY_REQS_STS_RSN_TYP_CD

    from STAGING.STG_PAYMENT_REQUEST_CODE
    where PAY_MEDA_PREF_TYP_CD || PAY_REQS_TYP_CD || PAY_REQS_STT_TYP_CD || PAY_REQS_STS_TYP_CD || PAY_REQS_STS_RSN_TYP_CD is not null
    group by PAY_MEDA_PREF_TYP_CD || PAY_REQS_TYP_CD || PAY_REQS_STT_TYP_CD || PAY_REQS_STS_TYP_CD || PAY_REQS_STS_RSN_TYP_CD
    having count(*) > 1

) validation_errors


