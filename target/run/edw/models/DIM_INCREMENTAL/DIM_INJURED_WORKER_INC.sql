
        
        
    

    

    merge into DEV_EDW_32600145.DIM_INCREMENTAL.DIM_INJURED_WORKER_INC as DBT_INTERNAL_DEST
        using DEV_EDW_32600145.DIM_INCREMENTAL.DIM_INJURED_WORKER_INC__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.INJURED_WORKER_HKEY = DBT_INTERNAL_DEST.INJURED_WORKER_HKEY
        

    
    when matched then update set
        "INJURED_WORKER_HKEY" = DBT_INTERNAL_SOURCE."INJURED_WORKER_HKEY","UNIQUE_ID_KEY" = DBT_INTERNAL_SOURCE."UNIQUE_ID_KEY","CUSTOMER_NUMBER" = DBT_INTERNAL_SOURCE."CUSTOMER_NUMBER","COURTESY_TITLE_NAME" = DBT_INTERNAL_SOURCE."COURTESY_TITLE_NAME","FIRST_NAME" = DBT_INTERNAL_SOURCE."FIRST_NAME","MIDDLE_NAME" = DBT_INTERNAL_SOURCE."MIDDLE_NAME","LAST_NAME" = DBT_INTERNAL_SOURCE."LAST_NAME","SUFFIX_NAME" = DBT_INTERNAL_SOURCE."SUFFIX_NAME","FULL_NAME" = DBT_INTERNAL_SOURCE."FULL_NAME","GENDER_TYPE_CODE" = DBT_INTERNAL_SOURCE."GENDER_TYPE_CODE","GENDER_TYPE_DESC" = DBT_INTERNAL_SOURCE."GENDER_TYPE_DESC","MARITAL_STATUS_TYPE_DESC" = DBT_INTERNAL_SOURCE."MARITAL_STATUS_TYPE_DESC","BIRTH_DATE" = DBT_INTERNAL_SOURCE."BIRTH_DATE","RETIREMENT_DATE" = DBT_INTERNAL_SOURCE."RETIREMENT_DATE","DEATH_DATE" = DBT_INTERNAL_SOURCE."DEATH_DATE","FOREIGN_CITIZEN_IND" = DBT_INTERNAL_SOURCE."FOREIGN_CITIZEN_IND","PRIMARY_LANGUAGE_TYPE_DESC" = DBT_INTERNAL_SOURCE."PRIMARY_LANGUAGE_TYPE_DESC","DOCUMENT_BLOCK_IND" = DBT_INTERNAL_SOURCE."DOCUMENT_BLOCK_IND","THREATENING_BEHAVIOR_BLOCK_IND" = DBT_INTERNAL_SOURCE."THREATENING_BEHAVIOR_BLOCK_IND","PHYSICAL_STREET_ADDRESS_1" = DBT_INTERNAL_SOURCE."PHYSICAL_STREET_ADDRESS_1","PHYSICAL_STREET_ADDRESS_2" = DBT_INTERNAL_SOURCE."PHYSICAL_STREET_ADDRESS_2","PHYSICAL_ADDRESS_CITY_NAME" = DBT_INTERNAL_SOURCE."PHYSICAL_ADDRESS_CITY_NAME","PHYSICAL_ADDRESS_STATE_CODE" = DBT_INTERNAL_SOURCE."PHYSICAL_ADDRESS_STATE_CODE","PHYSICAL_ADDRESS_STATE_NAME" = DBT_INTERNAL_SOURCE."PHYSICAL_ADDRESS_STATE_NAME","PHYSICAL_ADDRESS_POSTAL_CODE" = DBT_INTERNAL_SOURCE."PHYSICAL_ADDRESS_POSTAL_CODE","PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE" = DBT_INTERNAL_SOURCE."PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE","PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE" = DBT_INTERNAL_SOURCE."PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE","PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE" = DBT_INTERNAL_SOURCE."PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE","PHYSICAL_ADDRESS_COUNTY_NAME" = DBT_INTERNAL_SOURCE."PHYSICAL_ADDRESS_COUNTY_NAME","PHYSICAL_ADDRESS_COUNTRY_NAME" = DBT_INTERNAL_SOURCE."PHYSICAL_ADDRESS_COUNTRY_NAME","PHYSICAL_ADDRESS_VALIDATED_IND" = DBT_INTERNAL_SOURCE."PHYSICAL_ADDRESS_VALIDATED_IND","PHYSICAL_ADDRESS_COMMENT_TEXT" = DBT_INTERNAL_SOURCE."PHYSICAL_ADDRESS_COMMENT_TEXT","MAILING_STREET_ADDRESS_1" = DBT_INTERNAL_SOURCE."MAILING_STREET_ADDRESS_1","MAILING_STREET_ADDRESS_2" = DBT_INTERNAL_SOURCE."MAILING_STREET_ADDRESS_2","MAILING_ADDRESS_CITY_NAME" = DBT_INTERNAL_SOURCE."MAILING_ADDRESS_CITY_NAME","MAILING_ADDRESS_STATE_CODE" = DBT_INTERNAL_SOURCE."MAILING_ADDRESS_STATE_CODE","MAILING_ADDRESS_STATE_NAME" = DBT_INTERNAL_SOURCE."MAILING_ADDRESS_STATE_NAME","MAILING_ADDRESS_POSTAL_CODE" = DBT_INTERNAL_SOURCE."MAILING_ADDRESS_POSTAL_CODE","MAILING_FORMATTED_ADDRESS_POSTAL_CODE" = DBT_INTERNAL_SOURCE."MAILING_FORMATTED_ADDRESS_POSTAL_CODE","MAILING_FORMATTED_ADDRESS_ZIP_CODE" = DBT_INTERNAL_SOURCE."MAILING_FORMATTED_ADDRESS_ZIP_CODE","MAILING_FORMATTED_ADDRESS_ZIP4_CODE" = DBT_INTERNAL_SOURCE."MAILING_FORMATTED_ADDRESS_ZIP4_CODE","MAILING_ADDRESS_COUNTY_NAME" = DBT_INTERNAL_SOURCE."MAILING_ADDRESS_COUNTY_NAME","MAILING_ADDRESS_COUNTRY_NAME" = DBT_INTERNAL_SOURCE."MAILING_ADDRESS_COUNTRY_NAME","MAILING_ADDRESS_VALIDATED_IND" = DBT_INTERNAL_SOURCE."MAILING_ADDRESS_VALIDATED_IND","MAILING_ADDRESS_COMMENT_TEXT" = DBT_INTERNAL_SOURCE."MAILING_ADDRESS_COMMENT_TEXT","PARTICIPATION_TYPE_CODE" = DBT_INTERNAL_SOURCE."PARTICIPATION_TYPE_CODE","PARTICIPATION_TYPE_DESC" = DBT_INTERNAL_SOURCE."PARTICIPATION_TYPE_DESC","CURRENT_RECORD_IND" = DBT_INTERNAL_SOURCE."CURRENT_RECORD_IND","RECORD_EFFECTIVE_DATE" = DBT_INTERNAL_SOURCE."RECORD_EFFECTIVE_DATE","RECORD_END_DATE" = DBT_INTERNAL_SOURCE."RECORD_END_DATE","LOAD_DATETIME" = DBT_INTERNAL_SOURCE."LOAD_DATETIME","UPDATE_DATETIME" = DBT_INTERNAL_SOURCE."UPDATE_DATETIME","PRIMARY_SOURCE_SYSTEM" = DBT_INTERNAL_SOURCE."PRIMARY_SOURCE_SYSTEM"
    

    when not matched then insert
        ("INJURED_WORKER_HKEY", "UNIQUE_ID_KEY", "CUSTOMER_NUMBER", "COURTESY_TITLE_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "SUFFIX_NAME", "FULL_NAME", "GENDER_TYPE_CODE", "GENDER_TYPE_DESC", "MARITAL_STATUS_TYPE_DESC", "BIRTH_DATE", "RETIREMENT_DATE", "DEATH_DATE", "FOREIGN_CITIZEN_IND", "PRIMARY_LANGUAGE_TYPE_DESC", "DOCUMENT_BLOCK_IND", "THREATENING_BEHAVIOR_BLOCK_IND", "PHYSICAL_STREET_ADDRESS_1", "PHYSICAL_STREET_ADDRESS_2", "PHYSICAL_ADDRESS_CITY_NAME", "PHYSICAL_ADDRESS_STATE_CODE", "PHYSICAL_ADDRESS_STATE_NAME", "PHYSICAL_ADDRESS_POSTAL_CODE", "PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE", "PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE", "PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE", "PHYSICAL_ADDRESS_COUNTY_NAME", "PHYSICAL_ADDRESS_COUNTRY_NAME", "PHYSICAL_ADDRESS_VALIDATED_IND", "PHYSICAL_ADDRESS_COMMENT_TEXT", "MAILING_STREET_ADDRESS_1", "MAILING_STREET_ADDRESS_2", "MAILING_ADDRESS_CITY_NAME", "MAILING_ADDRESS_STATE_CODE", "MAILING_ADDRESS_STATE_NAME", "MAILING_ADDRESS_POSTAL_CODE", "MAILING_FORMATTED_ADDRESS_POSTAL_CODE", "MAILING_FORMATTED_ADDRESS_ZIP_CODE", "MAILING_FORMATTED_ADDRESS_ZIP4_CODE", "MAILING_ADDRESS_COUNTY_NAME", "MAILING_ADDRESS_COUNTRY_NAME", "MAILING_ADDRESS_VALIDATED_IND", "MAILING_ADDRESS_COMMENT_TEXT", "PARTICIPATION_TYPE_CODE", "PARTICIPATION_TYPE_DESC", "CURRENT_RECORD_IND", "RECORD_EFFECTIVE_DATE", "RECORD_END_DATE", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
    values
        ("INJURED_WORKER_HKEY", "UNIQUE_ID_KEY", "CUSTOMER_NUMBER", "COURTESY_TITLE_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "SUFFIX_NAME", "FULL_NAME", "GENDER_TYPE_CODE", "GENDER_TYPE_DESC", "MARITAL_STATUS_TYPE_DESC", "BIRTH_DATE", "RETIREMENT_DATE", "DEATH_DATE", "FOREIGN_CITIZEN_IND", "PRIMARY_LANGUAGE_TYPE_DESC", "DOCUMENT_BLOCK_IND", "THREATENING_BEHAVIOR_BLOCK_IND", "PHYSICAL_STREET_ADDRESS_1", "PHYSICAL_STREET_ADDRESS_2", "PHYSICAL_ADDRESS_CITY_NAME", "PHYSICAL_ADDRESS_STATE_CODE", "PHYSICAL_ADDRESS_STATE_NAME", "PHYSICAL_ADDRESS_POSTAL_CODE", "PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE", "PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE", "PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE", "PHYSICAL_ADDRESS_COUNTY_NAME", "PHYSICAL_ADDRESS_COUNTRY_NAME", "PHYSICAL_ADDRESS_VALIDATED_IND", "PHYSICAL_ADDRESS_COMMENT_TEXT", "MAILING_STREET_ADDRESS_1", "MAILING_STREET_ADDRESS_2", "MAILING_ADDRESS_CITY_NAME", "MAILING_ADDRESS_STATE_CODE", "MAILING_ADDRESS_STATE_NAME", "MAILING_ADDRESS_POSTAL_CODE", "MAILING_FORMATTED_ADDRESS_POSTAL_CODE", "MAILING_FORMATTED_ADDRESS_ZIP_CODE", "MAILING_FORMATTED_ADDRESS_ZIP4_CODE", "MAILING_ADDRESS_COUNTY_NAME", "MAILING_ADDRESS_COUNTRY_NAME", "MAILING_ADDRESS_VALIDATED_IND", "MAILING_ADDRESS_COMMENT_TEXT", "PARTICIPATION_TYPE_CODE", "PARTICIPATION_TYPE_DESC", "CURRENT_RECORD_IND", "RECORD_EFFECTIVE_DATE", "RECORD_END_DATE", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
