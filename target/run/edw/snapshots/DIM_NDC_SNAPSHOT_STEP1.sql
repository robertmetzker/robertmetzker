
      merge into "DEV_EDW"."EDW_STAGING_SNAPSHOT"."DIM_NDC_SNAPSHOT_STEP1" as DBT_INTERNAL_DEST
    using "DEV_EDW"."EDW_STAGING_SNAPSHOT"."DIM_NDC_SNAPSHOT_STEP1__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("GPI_14_CODE", "GPI_14_DESC", "GPI_2_GROUP_CODE", "GPI_2_GROUP_DESC", "GPI_4_CLASS_CODE", "GPI_4_CLASS_DESC", "GPI_6_SUBCLASS_CODE", "GPI_6_SUBCLASS_DESC", "GPI_8_DRUG_NAME_CODE", "GPI_8_DRUG_NAME_DESC", "GPI_10_DRUG_NAME_EXT_CODE", "GPI_10_DRUG_NAME_EXT_DESC", "GPI_12_DRUG_DOSAGE_FORM_CODE", "GPI_12_DRUG_DOSAGE_FORM_DESC", "GPI_EFFECTIVE_DATE", "GPI_END_DATE", "UNIQUE_ID_KEY", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("GPI_14_CODE", "GPI_14_DESC", "GPI_2_GROUP_CODE", "GPI_2_GROUP_DESC", "GPI_4_CLASS_CODE", "GPI_4_CLASS_DESC", "GPI_6_SUBCLASS_CODE", "GPI_6_SUBCLASS_DESC", "GPI_8_DRUG_NAME_CODE", "GPI_8_DRUG_NAME_DESC", "GPI_10_DRUG_NAME_EXT_CODE", "GPI_10_DRUG_NAME_EXT_DESC", "GPI_12_DRUG_DOSAGE_FORM_CODE", "GPI_12_DRUG_DOSAGE_FORM_DESC", "GPI_EFFECTIVE_DATE", "GPI_END_DATE", "UNIQUE_ID_KEY", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
    ;

  