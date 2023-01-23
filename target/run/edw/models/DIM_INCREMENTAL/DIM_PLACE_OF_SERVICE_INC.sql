
        
        
    

    

    merge into DEV_EDW_32600145.DIM_INCREMENTAL.DIM_PLACE_OF_SERVICE_INC as DBT_INTERNAL_DEST
        using DEV_EDW_32600145.DIM_INCREMENTAL.DIM_PLACE_OF_SERVICE_INC__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.PLACE_OF_SERVICE_HKEY = DBT_INTERNAL_DEST.PLACE_OF_SERVICE_HKEY
        

    
    when matched then update set
        "PLACE_OF_SERVICE_HKEY" = DBT_INTERNAL_SOURCE."PLACE_OF_SERVICE_HKEY","UNIQUE_ID_KEY" = DBT_INTERNAL_SOURCE."UNIQUE_ID_KEY","LINE_PLACE_OF_SERVICE_CODE" = DBT_INTERNAL_SOURCE."LINE_PLACE_OF_SERVICE_CODE","LINE_PLACE_OF_SERVICE_DESC" = DBT_INTERNAL_SOURCE."LINE_PLACE_OF_SERVICE_DESC","OUT_OF_OFFICE_IND" = DBT_INTERNAL_SOURCE."OUT_OF_OFFICE_IND","PLACE_OF_SERVICE_EFFECTIVE_DATE" = DBT_INTERNAL_SOURCE."PLACE_OF_SERVICE_EFFECTIVE_DATE","PLACE_OF_SERVICE_END_DATE" = DBT_INTERNAL_SOURCE."PLACE_OF_SERVICE_END_DATE","CURRENT_RECORD_IND" = DBT_INTERNAL_SOURCE."CURRENT_RECORD_IND","RECORD_EFFECTIVE_DATE" = DBT_INTERNAL_SOURCE."RECORD_EFFECTIVE_DATE","RECORD_END_DATE" = DBT_INTERNAL_SOURCE."RECORD_END_DATE","LOAD_DATETIME" = DBT_INTERNAL_SOURCE."LOAD_DATETIME","UPDATE_DATETIME" = DBT_INTERNAL_SOURCE."UPDATE_DATETIME","PRIMARY_SOURCE_SYSTEM" = DBT_INTERNAL_SOURCE."PRIMARY_SOURCE_SYSTEM"
    

    when not matched then insert
        ("PLACE_OF_SERVICE_HKEY", "UNIQUE_ID_KEY", "LINE_PLACE_OF_SERVICE_CODE", "LINE_PLACE_OF_SERVICE_DESC", "OUT_OF_OFFICE_IND", "PLACE_OF_SERVICE_EFFECTIVE_DATE", "PLACE_OF_SERVICE_END_DATE", "CURRENT_RECORD_IND", "RECORD_EFFECTIVE_DATE", "RECORD_END_DATE", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
    values
        ("PLACE_OF_SERVICE_HKEY", "UNIQUE_ID_KEY", "LINE_PLACE_OF_SERVICE_CODE", "LINE_PLACE_OF_SERVICE_DESC", "OUT_OF_OFFICE_IND", "PLACE_OF_SERVICE_EFFECTIVE_DATE", "PLACE_OF_SERVICE_END_DATE", "CURRENT_RECORD_IND", "RECORD_EFFECTIVE_DATE", "RECORD_END_DATE", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
