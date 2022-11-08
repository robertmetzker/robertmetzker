
    
    



select count(*) as validation_errors
from (

    select
        ACNTB_CODE||'-'||PYMNT_FUND_TYPE||'-'||CVRG_TYPE||'-'||BILL_TYPE_F2||'-'||BILL_TYPE_L3||'-'||ACDNT_TYPE||'-'||STS_CODE

    from STAGING.DST_PAYMENT_CODER
    where ACNTB_CODE||'-'||PYMNT_FUND_TYPE||'-'||CVRG_TYPE||'-'||BILL_TYPE_F2||'-'||BILL_TYPE_L3||'-'||ACDNT_TYPE||'-'||STS_CODE is not null
    group by ACNTB_CODE||'-'||PYMNT_FUND_TYPE||'-'||CVRG_TYPE||'-'||BILL_TYPE_F2||'-'||BILL_TYPE_L3||'-'||ACDNT_TYPE||'-'||STS_CODE
    having count(*) > 1

) validation_errors


