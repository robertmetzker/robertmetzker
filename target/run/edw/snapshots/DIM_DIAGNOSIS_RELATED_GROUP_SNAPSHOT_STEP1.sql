
      merge into "DEV_EDW"."EDW_STAGING_SNAPSHOT"."DIM_DIAGNOSIS_RELATED_GROUP_SNAPSHOT_STEP1" as DBT_INTERNAL_DEST
    using "DEV_EDW"."EDW_STAGING_SNAPSHOT"."DIM_DIAGNOSIS_RELATED_GROUP_SNAPSHOT_STEP1__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("DIAGNOSIS_RELATED_GROUP_TITLE", "DRG_EFFECTIVE_DATE", "DRG_EXPIRATION_DATE", "UNIQUE_ID_KEY", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("DIAGNOSIS_RELATED_GROUP_TITLE", "DRG_EFFECTIVE_DATE", "DRG_EXPIRATION_DATE", "UNIQUE_ID_KEY", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
    ;

  