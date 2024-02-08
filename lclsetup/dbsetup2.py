config = {
    'env': {
        'dev': {
            'bronze': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'firstname.lastname@fbwinn.com' ,
                'database': 'EDP_BRONZE_DEV' ,
                'role': 'DATALOAD_DEV' ,
                'warehouse': 'WH_DATALOAD_DEV' , 
                'authenticator': 'externalbrowser' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
            'personal': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'firstname.lastname@fbwinn.com' ,
                'database': 'PLAYRGROUND' ,
                'role': 'DATALOAD_DEV' ,
                'warehouse': 'WH_DATALOAD_DEV' , 
                'schema':'FIRST_LAST',
                'authenticator': 'externalbrowser' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
            'fbronze': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'dbfunct' ,
                'password': 'xxxxxxx',
                'database': 'EDP_BRONZE_DEV' ,
                'role': 'ACCOUNTADMIN' ,
                'warehouse': 'WH_DATALOAD_DEV' , 
                'authenticator': 'snowflake' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
            'funct': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'dbfunct' ,
                'password': 'xxxxxxx',
                'database': 'ROLLBACK' ,
                'role': 'ACCOUNTADMIN' ,
                'warehouse': 'WH_DATALOAD_DEV' , 
                'authenticator': 'snowflake' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
            'rollback': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'firstname.lastname@fbwinn.com' ,
                'database': 'ROLLBACK' ,
                'role': 'ACCOUNTADMIN' ,
                'warehouse': 'WH_DATALOAD_PROD' , 
                'authenticator': 'externalbrowser' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
        },
        'uat': {
            'bronze': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'firstname.lastname@fbwinn.com' ,
                'database': 'EDP_BRONZE_QA' ,
                'role': 'DATALOAD_QA' ,
                'warehouse': 'WH_DATALOAD_QA' , 
                'authenticator': 'externalbrowser' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
        },
        'prd': {
            'bronze': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'firstname.lastname@fbwinn.com' ,
                'database': 'EDP_BRONZE_PROD' ,
                'role': 'DATALOAD_PROD' ,
                'warehouse': 'WH_DATALOAD_PROD' , 
                'authenticator': 'externalbrowser' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
        },
        'rb': {
            'rollback': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'firstname.lastname@fbwinn.com' ,
                'database': 'ROLLBACK' ,
                'role': 'ACCOUNTADMIN' ,
                'warehouse': 'WH_DATALOAD_PROD' , 
                'authenticator': 'externalbrowser' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
        },
    }
},
