
        
        
    

    

    merge into DEV_EDW_32600145.DIM_INCREMENTAL.DIM_EOB_INC as DBT_INTERNAL_DEST
        using DEV_EDW_32600145.DIM_INCREMENTAL.DIM_EOB_INC__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.EOB_HKEY = DBT_INTERNAL_DEST.EOB_HKEY
        

    
    when matched then update set
        "EOB_HKEY" = DBT_INTERNAL_SOURCE."EOB_HKEY","UNIQUE_ID_KEY" = DBT_INTERNAL_SOURCE."UNIQUE_ID_KEY","EOB_CODE" = DBT_INTERNAL_SOURCE."EOB_CODE","EOB_SHORT_DESCRIPTION" = DBT_INTERNAL_SOURCE."EOB_SHORT_DESCRIPTION","EOB_LONG_DESCRIPTION" = DBT_INTERNAL_SOURCE."EOB_LONG_DESCRIPTION","EOB_CATEGORY_CODE" = DBT_INTERNAL_SOURCE."EOB_CATEGORY_CODE","EOB_CATEGORY_DESCRIPTION" = DBT_INTERNAL_SOURCE."EOB_CATEGORY_DESCRIPTION","EOB_TYPE_CODE" = DBT_INTERNAL_SOURCE."EOB_TYPE_CODE","EOB_TYPE_DESCRIPTION" = DBT_INTERNAL_SOURCE."EOB_TYPE_DESCRIPTION","EOB_EFFECTIVE_DATE" = DBT_INTERNAL_SOURCE."EOB_EFFECTIVE_DATE","EOB_END_DATE" = DBT_INTERNAL_SOURCE."EOB_END_DATE","APPLIED_DESC" = DBT_INTERNAL_SOURCE."APPLIED_DESC","CURRENT_RECORD_IND" = DBT_INTERNAL_SOURCE."CURRENT_RECORD_IND","RECORD_EFFECTIVE_DATE" = DBT_INTERNAL_SOURCE."RECORD_EFFECTIVE_DATE","RECORD_END_DATE" = DBT_INTERNAL_SOURCE."RECORD_END_DATE","LOAD_DATETIME" = DBT_INTERNAL_SOURCE."LOAD_DATETIME","UPDATE_DATETIME" = DBT_INTERNAL_SOURCE."UPDATE_DATETIME","PRIMARY_SOURCE_SYSTEM" = DBT_INTERNAL_SOURCE."PRIMARY_SOURCE_SYSTEM"
    

    when not matched then insert
        ("EOB_HKEY", "UNIQUE_ID_KEY", "EOB_CODE", "EOB_SHORT_DESCRIPTION", "EOB_LONG_DESCRIPTION", "EOB_CATEGORY_CODE", "EOB_CATEGORY_DESCRIPTION", "EOB_TYPE_CODE", "EOB_TYPE_DESCRIPTION", "EOB_EFFECTIVE_DATE", "EOB_END_DATE", "APPLIED_DESC", "CURRENT_RECORD_IND", "RECORD_EFFECTIVE_DATE", "RECORD_END_DATE", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
    values
        ("EOB_HKEY", "UNIQUE_ID_KEY", "EOB_CODE", "EOB_SHORT_DESCRIPTION", "EOB_LONG_DESCRIPTION", "EOB_CATEGORY_CODE", "EOB_CATEGORY_DESCRIPTION", "EOB_TYPE_CODE", "EOB_TYPE_DESCRIPTION", "EOB_EFFECTIVE_DATE", "EOB_END_DATE", "APPLIED_DESC", "CURRENT_RECORD_IND", "RECORD_EFFECTIVE_DATE", "RECORD_END_DATE", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
