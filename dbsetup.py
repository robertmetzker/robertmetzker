config = {
    'env': {
        'gpd': {
            'dev': {
                'dbtype': 'sqlserver',
                'server': 'mant6sqlsrv3',
                'port': '1433',
                'database': 'TLI',
                'schema':'dbo',
                'user': 'fivetran',
                'password': 'T3VyODAyMERlYWw=',
                }
            },
        'tt': {
            'E21BSE': {
                'dbtype': 'oracle',
                'host': 'dmaslorcl01',
                'port': '1521',
                'service_name': 'e21bse.masl10devsrv1',
                'user': 'fivetran',
                'password': 'b3VyODAyMGRlYWw=',
                }
            },
        'moen': {
            'dev': {
                'dbtype': 'oracle',
                'host': 'phxdevdb01.fbin.oci',
                'port': '1521',
                'service_name': 'e21bse.masl10devsrv1',
                'user': 'APPSBI',
                'password': 'U3dvcmRmaXNoIzEy',
                }
            },
        'mlock': {
            'RDMLCKI': {
                'dbtype': 'oracle',
                'host': 'diatmlckidb01.tmlc.ras',
                'port': '1521',
                'service_name': 'DMLCKI',
                'user': 'RAC_ACCNT',
                'password': 'a1Z5c2hfQXN0MDRy',
                }
            },
        'dr': {
            'funct': {
                'dbtype': 'snowflake',
                'account': 'fbhs-gpg_dr.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'dbfunct' ,
                'password': 'NDI0N1hTMnNub3dmbGFrZSE=',
                'database': 'EDP_BRONZE_DEV' ,
                'role': 'ACCOUNTADMIN' ,
                'warehouse': 'WH_DATALOAD_DEV' , 
                'authenticator': 'snowflake' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
         },
        'dev': {
            'bronze': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'robert.metzker@fbwinn.com' ,
                'database': 'EDP_BRONZE_DEV' ,
                'role': 'DATALOAD_DEV' ,
                'warehouse': 'WH_DATALOAD_DEV' , 
                'authenticator': 'externalbrowser' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
            'personal': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'robert.metzker@fbwinn.com' ,
                'database': 'PLAYRGROUND' ,
                'role': 'DATALOAD_DEV' ,
                'warehouse': 'WH_DATALOAD_DEV' , 
                'schema':'ROBERT_METZKER',
                'authenticator': 'externalbrowser' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
            'fbronze': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'dbfunct' ,
                'password': 'NDI0N1hTMnNub3dmbGFrZSE=',
                'database': 'EDP_BRONZE_DEV' ,
                'role': 'ACCOUNTADMIN' ,
                'warehouse': 'TRANSFORM_MED' , 
                'authenticator': 'snowflake' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
            'funct': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'dbfunct' ,
                'password': 'NDI0N1hTMnNub3dmbGFrZSE=',
                'database': 'ROLLBACK' ,
                'role': 'ACCOUNTADMIN' ,
                'warehouse': 'WH_DATALOAD_DEV' , 
                'authenticator': 'snowflake' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
            'rollback': {
                'dbtype': 'snowflake',
                'account': 'fbhs_gpg.east-us-2.azure' , # https://fbhs_gpg.east-us-2.azure.snowflakecomputing.com/fed/login
                'user': 'robert.metzker@fbwinn.com' ,
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
                'user': 'robert.metzker@fbwinn.com' ,
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
                'user': 'robert.metzker@fbwinn.com' ,
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
                'user': 'robert.metzker@fbwinn.com' ,
                'database': 'ROLLBACK' ,
                'role': 'ACCOUNTADMIN' ,
                'warehouse': 'WH_DATALOAD_PROD' , 
                'authenticator': 'externalbrowser' , # externalbrowser, snowflake``
                'CLIENT_SESSION_KEEP_ALIVE': True , # 3600 default heartbeat freq in sec
            },
        },
    }
}