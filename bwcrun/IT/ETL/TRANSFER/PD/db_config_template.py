db_config = {
    "DEV": {
        "Oracle": {
            "db_type": "Oracle",
            "user": "<username>",
            "password": "<password>",
            "host": "<host>",
            "port": "<port>",
            "database": "<database>"
        },
        "Snowflake": {
            "db_type": "Snowflake",
            "user": "<username>",
            "password": "<password>",
            "account": "<account>",
            "warehouse": "<warehouse>",
            "database": "<database>",
            "schema": "<schema>"
        },
        "SQLServer": {
            "db_type": "SQLServer",
            "user": "<username>",
            "password": "<password>",
            "host": "<host>",
            "database": "<database>"
        }
    },
    "QA": {
        # Similar structure as DEV
    },
    "PROD": {
        # Similar structure as DEV
    }
}