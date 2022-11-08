
        
        
    

    

    merge into DEV_EDW_32600145.DIM_INCREMENTAL.DIM_PAYMENT_CODER_INC as DBT_INTERNAL_DEST
        using DEV_EDW_32600145.DIM_INCREMENTAL.DIM_PAYMENT_CODER_INC__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.PAYMENT_CODER_HKEY = DBT_INTERNAL_DEST.PAYMENT_CODER_HKEY
        

    
    when matched then update set
        "PAYMENT_CODER_HKEY" = DBT_INTERNAL_SOURCE."PAYMENT_CODER_HKEY","UNIQUE_ID_KEY" = DBT_INTERNAL_SOURCE."UNIQUE_ID_KEY","ACCOUNTABILITY_CODE" = DBT_INTERNAL_SOURCE."ACCOUNTABILITY_CODE","PAYMENT_FUND_TYPE_CODE" = DBT_INTERNAL_SOURCE."PAYMENT_FUND_TYPE_CODE","COVERAGE_TYPE_CODE" = DBT_INTERNAL_SOURCE."COVERAGE_TYPE_CODE","BILL_TYPE_F2_CODE" = DBT_INTERNAL_SOURCE."BILL_TYPE_F2_CODE","BILL_TYPE_L3_CODE" = DBT_INTERNAL_SOURCE."BILL_TYPE_L3_CODE","ACCIDENT_TYPE_CODE" = DBT_INTERNAL_SOURCE."ACCIDENT_TYPE_CODE","PAYMENT_STATUS_CODE" = DBT_INTERNAL_SOURCE."PAYMENT_STATUS_CODE","ACCOUNTABILITY_DESC" = DBT_INTERNAL_SOURCE."ACCOUNTABILITY_DESC","PAYMENT_FUND_DESC" = DBT_INTERNAL_SOURCE."PAYMENT_FUND_DESC","COVERAGE_DESC" = DBT_INTERNAL_SOURCE."COVERAGE_DESC","BILL_TYPE_F2_DESC" = DBT_INTERNAL_SOURCE."BILL_TYPE_F2_DESC","BILL_TYPE_L3_DESC" = DBT_INTERNAL_SOURCE."BILL_TYPE_L3_DESC","ACCIDENT_TYPE_DESC" = DBT_INTERNAL_SOURCE."ACCIDENT_TYPE_DESC","PAYMENT_STATUS_DESC" = DBT_INTERNAL_SOURCE."PAYMENT_STATUS_DESC","LOAD_DATETIME" = DBT_INTERNAL_SOURCE."LOAD_DATETIME","UPDATE_DATETIME" = DBT_INTERNAL_SOURCE."UPDATE_DATETIME","PRIMARY_SOURCE_SYSTEM" = DBT_INTERNAL_SOURCE."PRIMARY_SOURCE_SYSTEM"
    

    when not matched then insert
        ("PAYMENT_CODER_HKEY", "UNIQUE_ID_KEY", "ACCOUNTABILITY_CODE", "PAYMENT_FUND_TYPE_CODE", "COVERAGE_TYPE_CODE", "BILL_TYPE_F2_CODE", "BILL_TYPE_L3_CODE", "ACCIDENT_TYPE_CODE", "PAYMENT_STATUS_CODE", "ACCOUNTABILITY_DESC", "PAYMENT_FUND_DESC", "COVERAGE_DESC", "BILL_TYPE_F2_DESC", "BILL_TYPE_L3_DESC", "ACCIDENT_TYPE_DESC", "PAYMENT_STATUS_DESC", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
    values
        ("PAYMENT_CODER_HKEY", "UNIQUE_ID_KEY", "ACCOUNTABILITY_CODE", "PAYMENT_FUND_TYPE_CODE", "COVERAGE_TYPE_CODE", "BILL_TYPE_F2_CODE", "BILL_TYPE_L3_CODE", "ACCIDENT_TYPE_CODE", "PAYMENT_STATUS_CODE", "ACCOUNTABILITY_DESC", "PAYMENT_FUND_DESC", "COVERAGE_DESC", "BILL_TYPE_F2_DESC", "BILL_TYPE_L3_DESC", "ACCIDENT_TYPE_DESC", "PAYMENT_STATUS_DESC", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")

