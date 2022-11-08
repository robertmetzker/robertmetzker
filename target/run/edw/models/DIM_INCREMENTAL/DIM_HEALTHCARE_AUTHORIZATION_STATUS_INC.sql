
        
        
    

    

    merge into DEV_EDW_32600145.DIM_INCREMENTAL.DIM_HEALTHCARE_AUTHORIZATION_STATUS_INC as DBT_INTERNAL_DEST
        using DEV_EDW_32600145.DIM_INCREMENTAL.DIM_HEALTHCARE_AUTHORIZATION_STATUS_INC__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.HEALTHCARE_AUTHORIZATION_STATUS_HKEY = DBT_INTERNAL_DEST.HEALTHCARE_AUTHORIZATION_STATUS_HKEY
        

    
    when matched then update set
        "HEALTHCARE_AUTHORIZATION_STATUS_HKEY" = DBT_INTERNAL_SOURCE."HEALTHCARE_AUTHORIZATION_STATUS_HKEY","UNIQUE_ID_KEY" = DBT_INTERNAL_SOURCE."UNIQUE_ID_KEY","AUTHORIZATION_STATUS_CODE" = DBT_INTERNAL_SOURCE."AUTHORIZATION_STATUS_CODE","AUTHORIZATION_SERVICE_TYPE_CODE" = DBT_INTERNAL_SOURCE."AUTHORIZATION_SERVICE_TYPE_CODE","AUTHORIZATION_STATUS_DESC" = DBT_INTERNAL_SOURCE."AUTHORIZATION_STATUS_DESC","AUTHORIZATION_SERVICE_TYPE_DESC" = DBT_INTERNAL_SOURCE."AUTHORIZATION_SERVICE_TYPE_DESC","LOAD_DATETIME" = DBT_INTERNAL_SOURCE."LOAD_DATETIME","UPDATE_DATETIME" = DBT_INTERNAL_SOURCE."UPDATE_DATETIME","PRIMARY_SOURCE_SYSTEM" = DBT_INTERNAL_SOURCE."PRIMARY_SOURCE_SYSTEM"
    

    when not matched then insert
        ("HEALTHCARE_AUTHORIZATION_STATUS_HKEY", "UNIQUE_ID_KEY", "AUTHORIZATION_STATUS_CODE", "AUTHORIZATION_SERVICE_TYPE_CODE", "AUTHORIZATION_STATUS_DESC", "AUTHORIZATION_SERVICE_TYPE_DESC", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
    values
        ("HEALTHCARE_AUTHORIZATION_STATUS_HKEY", "UNIQUE_ID_KEY", "AUTHORIZATION_STATUS_CODE", "AUTHORIZATION_SERVICE_TYPE_CODE", "AUTHORIZATION_STATUS_DESC", "AUTHORIZATION_SERVICE_TYPE_DESC", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")

