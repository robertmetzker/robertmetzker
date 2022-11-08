
      merge into "DEV_EDW"."EDW_STAGING_SNAPSHOT"."DIM_REPRESENTATIVE_SNAPSHOT_STEP1" as DBT_INTERNAL_DEST
    using "DEV_EDW"."EDW_STAGING_SNAPSHOT"."DIM_REPRESENTATIVE_SNAPSHOT_STEP1__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("CUSTOMER_NAME", "DOCUMENT_BLOCK_IND", "THREAT_BEHAVIOR_BLOCK_IND", "MAILING_STREET_ADDRESS_1", "MAILING_STREET_ADDRESS_2", "MAILING_ADDRESS_CITY_NAME", "MAILING_ADDRESS_STATE_CODE", "MAILING_ADDRESS_STATE_NAME", "MAILING_ADDRESS_POSTAL_CODE", "MAILING_FORMATTED_ADDRESS_POSTAL_CODE", "MAILING_FORMATTED_ADDRESS_ZIP_CODE", "MAILING_FORMATTED_ADDRESS_ZIP4_CODE", "MAILING_ADDRESS_COUNTY_NAME", "MAILING_ADDRESS_COUNTRY_NAME", "MAILING_ADDRESS_VALIDATED_IND", "MAILING_ADDRESS_COMMENT_TEXT", "PHYSICAL_STREET_ADDRESS_1", "PHYSICAL_STREET_ADDRESS_2", "PHYSICAL_ADDRESS_CITY_NAME", "PHYSICAL_ADDRESS_STATE_CODE", "PHYSICAL_ADDRESS_STATE_NAME", "PHYSICAL_ADDRESS_POSTAL_CODE", "PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE", "PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE", "PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE", "PHYSICAL_ADDRESS_COUNTY_NAME", "PHYSICAL_ADDRESS_COUNTRY_NAME", "PHYSICAL_ADDRESS_VALIDATED_IND", "PHYSICAL_ADDRESS_COMMENT_TEXT", "UNIQUE_ID_KEY", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("CUSTOMER_NAME", "DOCUMENT_BLOCK_IND", "THREAT_BEHAVIOR_BLOCK_IND", "MAILING_STREET_ADDRESS_1", "MAILING_STREET_ADDRESS_2", "MAILING_ADDRESS_CITY_NAME", "MAILING_ADDRESS_STATE_CODE", "MAILING_ADDRESS_STATE_NAME", "MAILING_ADDRESS_POSTAL_CODE", "MAILING_FORMATTED_ADDRESS_POSTAL_CODE", "MAILING_FORMATTED_ADDRESS_ZIP_CODE", "MAILING_FORMATTED_ADDRESS_ZIP4_CODE", "MAILING_ADDRESS_COUNTY_NAME", "MAILING_ADDRESS_COUNTRY_NAME", "MAILING_ADDRESS_VALIDATED_IND", "MAILING_ADDRESS_COMMENT_TEXT", "PHYSICAL_STREET_ADDRESS_1", "PHYSICAL_STREET_ADDRESS_2", "PHYSICAL_ADDRESS_CITY_NAME", "PHYSICAL_ADDRESS_STATE_CODE", "PHYSICAL_ADDRESS_STATE_NAME", "PHYSICAL_ADDRESS_POSTAL_CODE", "PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE", "PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE", "PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE", "PHYSICAL_ADDRESS_COUNTY_NAME", "PHYSICAL_ADDRESS_COUNTRY_NAME", "PHYSICAL_ADDRESS_VALIDATED_IND", "PHYSICAL_ADDRESS_COMMENT_TEXT", "UNIQUE_ID_KEY", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
    ;

  