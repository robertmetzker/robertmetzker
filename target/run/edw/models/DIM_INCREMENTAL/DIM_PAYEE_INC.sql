
        
        
    

    

    merge into DEV_EDW_32600145.DIM_INCREMENTAL.DIM_PAYEE_INC as DBT_INTERNAL_DEST
        using DEV_EDW_32600145.DIM_INCREMENTAL.DIM_PAYEE_INC__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.PAYEE_HKEY = DBT_INTERNAL_DEST.PAYEE_HKEY
        

    
    when matched then update set
        "PAYEE_HKEY" = DBT_INTERNAL_SOURCE."PAYEE_HKEY","UNIQUE_ID_KEY" = DBT_INTERNAL_SOURCE."UNIQUE_ID_KEY","PAYEE_FULL_NAME" = DBT_INTERNAL_SOURCE."PAYEE_FULL_NAME","LOAD_DATETIME" = DBT_INTERNAL_SOURCE."LOAD_DATETIME","UPDATE_DATETIME" = DBT_INTERNAL_SOURCE."UPDATE_DATETIME","PRIMARY_SOURCE_SYSTEM" = DBT_INTERNAL_SOURCE."PRIMARY_SOURCE_SYSTEM"
    

    when not matched then insert
        ("PAYEE_HKEY", "UNIQUE_ID_KEY", "PAYEE_FULL_NAME", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
    values
        ("PAYEE_HKEY", "UNIQUE_ID_KEY", "PAYEE_FULL_NAME", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")

