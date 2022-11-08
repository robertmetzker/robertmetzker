





with validation_errors as (

    select
        PAY_MEDA_PREF_TYP_CD, PAY_REQS_TYP_CD, PAY_REQS_STT_TYP_CD, PAY_REQS_STS_TYP_CD, PAY_REQS_STS_RSN_TYP_CD
    from STAGING.DST_PAYMENT_REQUEST_CODE

    group by PAY_MEDA_PREF_TYP_CD, PAY_REQS_TYP_CD, PAY_REQS_STT_TYP_CD, PAY_REQS_STS_TYP_CD, PAY_REQS_STS_RSN_TYP_CD
    having count(*) > 1

)

select count(*)
from validation_errors


