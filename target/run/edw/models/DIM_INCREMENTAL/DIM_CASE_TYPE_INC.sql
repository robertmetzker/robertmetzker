
        
        
    

    

    merge into DEV_EDW.DIM_INCREMENTAL.DIM_CASE_TYPE_INC as DBT_INTERNAL_DEST
        using DEV_EDW.DIM_INCREMENTAL.DIM_CASE_TYPE_INC__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.CASE_TYPE_HKEY = DBT_INTERNAL_DEST.CASE_TYPE_HKEY
        

    
    when matched then update set
        "CASE_TYPE_HKEY" = DBT_INTERNAL_SOURCE."CASE_TYPE_HKEY","UNIQUE_ID_KEY" = DBT_INTERNAL_SOURCE."UNIQUE_ID_KEY","CASE_TYPE_CODE" = DBT_INTERNAL_SOURCE."CASE_TYPE_CODE","CASE_CATEGORY_CODE" = DBT_INTERNAL_SOURCE."CASE_CATEGORY_CODE","CASE_TYPE_DESC" = DBT_INTERNAL_SOURCE."CASE_TYPE_DESC","CASE_CATEGORY_DESC" = DBT_INTERNAL_SOURCE."CASE_CATEGORY_DESC","LOAD_DATETIME" = DBT_INTERNAL_SOURCE."LOAD_DATETIME","UPDATE_DATETIME" = DBT_INTERNAL_SOURCE."UPDATE_DATETIME","PRIMARY_SOURCE_SYSTEM" = DBT_INTERNAL_SOURCE."PRIMARY_SOURCE_SYSTEM"
    

    when not matched then insert
        ("CASE_TYPE_HKEY", "UNIQUE_ID_KEY", "CASE_TYPE_CODE", "CASE_CATEGORY_CODE", "CASE_TYPE_DESC", "CASE_CATEGORY_DESC", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
    values
        ("CASE_TYPE_HKEY", "UNIQUE_ID_KEY", "CASE_TYPE_CODE", "CASE_CATEGORY_CODE", "CASE_TYPE_DESC", "CASE_CATEGORY_DESC", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")

