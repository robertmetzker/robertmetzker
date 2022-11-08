
        
        
    

    

    merge into DEV_EDW_32600145.DIM_INCREMENTAL.DIM_NDC_INC as DBT_INTERNAL_DEST
        using DEV_EDW_32600145.DIM_INCREMENTAL.DIM_NDC_INC__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.NDC_GPI_HKEY = DBT_INTERNAL_DEST.NDC_GPI_HKEY
        

    
    when matched then update set
        "NDC_GPI_HKEY" = DBT_INTERNAL_SOURCE."NDC_GPI_HKEY","UNIQUE_ID_KEY" = DBT_INTERNAL_SOURCE."UNIQUE_ID_KEY","NDC_11_CODE" = DBT_INTERNAL_SOURCE."NDC_11_CODE","NDC_11_FORMATTED_CODE" = DBT_INTERNAL_SOURCE."NDC_11_FORMATTED_CODE","NDC_VERSION_NUMBER" = DBT_INTERNAL_SOURCE."NDC_VERSION_NUMBER","NDC_5_LABELER_CODE" = DBT_INTERNAL_SOURCE."NDC_5_LABELER_CODE","NDC_5_LABELER_NAME" = DBT_INTERNAL_SOURCE."NDC_5_LABELER_NAME","NDC_4_PRODUCT_CODE" = DBT_INTERNAL_SOURCE."NDC_4_PRODUCT_CODE","NDC_4_PRODUCT_DESC" = DBT_INTERNAL_SOURCE."NDC_4_PRODUCT_DESC","NDC_2_PACKAGE_CODE" = DBT_INTERNAL_SOURCE."NDC_2_PACKAGE_CODE","GPI_14_CODE" = DBT_INTERNAL_SOURCE."GPI_14_CODE","GPI_14_DESC" = DBT_INTERNAL_SOURCE."GPI_14_DESC","GPI_2_GROUP_CODE" = DBT_INTERNAL_SOURCE."GPI_2_GROUP_CODE","GPI_2_GROUP_DESC" = DBT_INTERNAL_SOURCE."GPI_2_GROUP_DESC","GPI_4_CLASS_CODE" = DBT_INTERNAL_SOURCE."GPI_4_CLASS_CODE","GPI_4_CLASS_DESC" = DBT_INTERNAL_SOURCE."GPI_4_CLASS_DESC","GPI_6_SUBCLASS_CODE" = DBT_INTERNAL_SOURCE."GPI_6_SUBCLASS_CODE","GPI_6_SUBCLASS_DESC" = DBT_INTERNAL_SOURCE."GPI_6_SUBCLASS_DESC","GPI_8_DRUG_NAME_CODE" = DBT_INTERNAL_SOURCE."GPI_8_DRUG_NAME_CODE","GPI_8_DRUG_NAME_DESC" = DBT_INTERNAL_SOURCE."GPI_8_DRUG_NAME_DESC","GPI_10_DRUG_NAME_EXT_CODE" = DBT_INTERNAL_SOURCE."GPI_10_DRUG_NAME_EXT_CODE","GPI_10_DRUG_NAME_EXT_DESC" = DBT_INTERNAL_SOURCE."GPI_10_DRUG_NAME_EXT_DESC","GPI_12_DRUG_DOSAGE_FORM_CODE" = DBT_INTERNAL_SOURCE."GPI_12_DRUG_DOSAGE_FORM_CODE","GPI_12_DRUG_DOSAGE_FORM_DESC" = DBT_INTERNAL_SOURCE."GPI_12_DRUG_DOSAGE_FORM_DESC","NDC_EFFECTIVE_DATE" = DBT_INTERNAL_SOURCE."NDC_EFFECTIVE_DATE","NDC_END_DATE" = DBT_INTERNAL_SOURCE."NDC_END_DATE","GPI_EFFECTIVE_DATE" = DBT_INTERNAL_SOURCE."GPI_EFFECTIVE_DATE","GPI_END_DATE" = DBT_INTERNAL_SOURCE."GPI_END_DATE","BWC_FORMULARY_PRIOR_AUTHORIZATION_IND" = DBT_INTERNAL_SOURCE."BWC_FORMULARY_PRIOR_AUTHORIZATION_IND","BWC_FORMULARY_COVERAGE_CODE" = DBT_INTERNAL_SOURCE."BWC_FORMULARY_COVERAGE_CODE","BWC_FORMULARY_COVERAGE_DESC" = DBT_INTERNAL_SOURCE."BWC_FORMULARY_COVERAGE_DESC","BWC_FORMULARY_COVERAGE_LIMITATION_DESC" = DBT_INTERNAL_SOURCE."BWC_FORMULARY_COVERAGE_LIMITATION_DESC","OVER_THE_COUNTER_CODE" = DBT_INTERNAL_SOURCE."OVER_THE_COUNTER_CODE","OVER_THE_COUNTER_DESC" = DBT_INTERNAL_SOURCE."OVER_THE_COUNTER_DESC","DEA_DRUG_SCHEDULE_CODE" = DBT_INTERNAL_SOURCE."DEA_DRUG_SCHEDULE_CODE","DEA_DRUG_SCHEDULE_DESC" = DBT_INTERNAL_SOURCE."DEA_DRUG_SCHEDULE_DESC","DRUG_STRENGTH_CODE" = DBT_INTERNAL_SOURCE."DRUG_STRENGTH_CODE","STRENGTH_MEASURE_CODE" = DBT_INTERNAL_SOURCE."STRENGTH_MEASURE_CODE","ROUTE_OF_ADMINISTRATION_CODE" = DBT_INTERNAL_SOURCE."ROUTE_OF_ADMINISTRATION_CODE","ROUTE_OF_ADMINISTRATION_DESC" = DBT_INTERNAL_SOURCE."ROUTE_OF_ADMINISTRATION_DESC","DOSAGE_FORM_CODE" = DBT_INTERNAL_SOURCE."DOSAGE_FORM_CODE","DOSAGE_FORM_DESC" = DBT_INTERNAL_SOURCE."DOSAGE_FORM_DESC","PACKAGE_SIZE_UNIT_OF_MEASURE_CODE" = DBT_INTERNAL_SOURCE."PACKAGE_SIZE_UNIT_OF_MEASURE_CODE","PACKAGE_SIZE_UNIT_OF_MEASURE_DESC" = DBT_INTERNAL_SOURCE."PACKAGE_SIZE_UNIT_OF_MEASURE_DESC","PACKAGE_TYPE_CODE" = DBT_INTERNAL_SOURCE."PACKAGE_TYPE_CODE","PACKAGE_TYPE_DESC" = DBT_INTERNAL_SOURCE."PACKAGE_TYPE_DESC","PACKAGE_DOSE_CODE" = DBT_INTERNAL_SOURCE."PACKAGE_DOSE_CODE","PACKAGE_DOSE_DESC" = DBT_INTERNAL_SOURCE."PACKAGE_DOSE_DESC","PACKAGE_SIZE_QTY" = DBT_INTERNAL_SOURCE."PACKAGE_SIZE_QTY","PACKAGE_QTY" = DBT_INTERNAL_SOURCE."PACKAGE_QTY","MILLIGRAMS_EQUIVALENCE_PER_UNIT" = DBT_INTERNAL_SOURCE."MILLIGRAMS_EQUIVALENCE_PER_UNIT","ACTIVE_DRUG_STRENGTH_QUANTITY" = DBT_INTERNAL_SOURCE."ACTIVE_DRUG_STRENGTH_QUANTITY","CURRENT_AWP_DRUG_AMOUNT" = DBT_INTERNAL_SOURCE."CURRENT_AWP_DRUG_AMOUNT","MED_CONVERSION_FACTOR" = DBT_INTERNAL_SOURCE."MED_CONVERSION_FACTOR","AED_CONVERSION_FACTOR" = DBT_INTERNAL_SOURCE."AED_CONVERSION_FACTOR","GPI_PAYMENT_CATEGORY_DESC" = DBT_INTERNAL_SOURCE."GPI_PAYMENT_CATEGORY_DESC","CURRENT_RECORD_IND" = DBT_INTERNAL_SOURCE."CURRENT_RECORD_IND","RECORD_EFFECTIVE_DATE" = DBT_INTERNAL_SOURCE."RECORD_EFFECTIVE_DATE","RECORD_END_DATE" = DBT_INTERNAL_SOURCE."RECORD_END_DATE","LOAD_DATETIME" = DBT_INTERNAL_SOURCE."LOAD_DATETIME","UPDATE_DATETIME" = DBT_INTERNAL_SOURCE."UPDATE_DATETIME","PRIMARY_SOURCE_SYSTEM" = DBT_INTERNAL_SOURCE."PRIMARY_SOURCE_SYSTEM"
    

    when not matched then insert
        ("NDC_GPI_HKEY", "UNIQUE_ID_KEY", "NDC_11_CODE", "NDC_11_FORMATTED_CODE", "NDC_VERSION_NUMBER", "NDC_5_LABELER_CODE", "NDC_5_LABELER_NAME", "NDC_4_PRODUCT_CODE", "NDC_4_PRODUCT_DESC", "NDC_2_PACKAGE_CODE", "GPI_14_CODE", "GPI_14_DESC", "GPI_2_GROUP_CODE", "GPI_2_GROUP_DESC", "GPI_4_CLASS_CODE", "GPI_4_CLASS_DESC", "GPI_6_SUBCLASS_CODE", "GPI_6_SUBCLASS_DESC", "GPI_8_DRUG_NAME_CODE", "GPI_8_DRUG_NAME_DESC", "GPI_10_DRUG_NAME_EXT_CODE", "GPI_10_DRUG_NAME_EXT_DESC", "GPI_12_DRUG_DOSAGE_FORM_CODE", "GPI_12_DRUG_DOSAGE_FORM_DESC", "NDC_EFFECTIVE_DATE", "NDC_END_DATE", "GPI_EFFECTIVE_DATE", "GPI_END_DATE", "BWC_FORMULARY_PRIOR_AUTHORIZATION_IND", "BWC_FORMULARY_COVERAGE_CODE", "BWC_FORMULARY_COVERAGE_DESC", "BWC_FORMULARY_COVERAGE_LIMITATION_DESC", "OVER_THE_COUNTER_CODE", "OVER_THE_COUNTER_DESC", "DEA_DRUG_SCHEDULE_CODE", "DEA_DRUG_SCHEDULE_DESC", "DRUG_STRENGTH_CODE", "STRENGTH_MEASURE_CODE", "ROUTE_OF_ADMINISTRATION_CODE", "ROUTE_OF_ADMINISTRATION_DESC", "DOSAGE_FORM_CODE", "DOSAGE_FORM_DESC", "PACKAGE_SIZE_UNIT_OF_MEASURE_CODE", "PACKAGE_SIZE_UNIT_OF_MEASURE_DESC", "PACKAGE_TYPE_CODE", "PACKAGE_TYPE_DESC", "PACKAGE_DOSE_CODE", "PACKAGE_DOSE_DESC", "PACKAGE_SIZE_QTY", "PACKAGE_QTY", "MILLIGRAMS_EQUIVALENCE_PER_UNIT", "ACTIVE_DRUG_STRENGTH_QUANTITY", "CURRENT_AWP_DRUG_AMOUNT", "MED_CONVERSION_FACTOR", "AED_CONVERSION_FACTOR", "GPI_PAYMENT_CATEGORY_DESC", "CURRENT_RECORD_IND", "RECORD_EFFECTIVE_DATE", "RECORD_END_DATE", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
    values
        ("NDC_GPI_HKEY", "UNIQUE_ID_KEY", "NDC_11_CODE", "NDC_11_FORMATTED_CODE", "NDC_VERSION_NUMBER", "NDC_5_LABELER_CODE", "NDC_5_LABELER_NAME", "NDC_4_PRODUCT_CODE", "NDC_4_PRODUCT_DESC", "NDC_2_PACKAGE_CODE", "GPI_14_CODE", "GPI_14_DESC", "GPI_2_GROUP_CODE", "GPI_2_GROUP_DESC", "GPI_4_CLASS_CODE", "GPI_4_CLASS_DESC", "GPI_6_SUBCLASS_CODE", "GPI_6_SUBCLASS_DESC", "GPI_8_DRUG_NAME_CODE", "GPI_8_DRUG_NAME_DESC", "GPI_10_DRUG_NAME_EXT_CODE", "GPI_10_DRUG_NAME_EXT_DESC", "GPI_12_DRUG_DOSAGE_FORM_CODE", "GPI_12_DRUG_DOSAGE_FORM_DESC", "NDC_EFFECTIVE_DATE", "NDC_END_DATE", "GPI_EFFECTIVE_DATE", "GPI_END_DATE", "BWC_FORMULARY_PRIOR_AUTHORIZATION_IND", "BWC_FORMULARY_COVERAGE_CODE", "BWC_FORMULARY_COVERAGE_DESC", "BWC_FORMULARY_COVERAGE_LIMITATION_DESC", "OVER_THE_COUNTER_CODE", "OVER_THE_COUNTER_DESC", "DEA_DRUG_SCHEDULE_CODE", "DEA_DRUG_SCHEDULE_DESC", "DRUG_STRENGTH_CODE", "STRENGTH_MEASURE_CODE", "ROUTE_OF_ADMINISTRATION_CODE", "ROUTE_OF_ADMINISTRATION_DESC", "DOSAGE_FORM_CODE", "DOSAGE_FORM_DESC", "PACKAGE_SIZE_UNIT_OF_MEASURE_CODE", "PACKAGE_SIZE_UNIT_OF_MEASURE_DESC", "PACKAGE_TYPE_CODE", "PACKAGE_TYPE_DESC", "PACKAGE_DOSE_CODE", "PACKAGE_DOSE_DESC", "PACKAGE_SIZE_QTY", "PACKAGE_QTY", "MILLIGRAMS_EQUIVALENCE_PER_UNIT", "ACTIVE_DRUG_STRENGTH_QUANTITY", "CURRENT_AWP_DRUG_AMOUNT", "MED_CONVERSION_FACTOR", "AED_CONVERSION_FACTOR", "GPI_PAYMENT_CATEGORY_DESC", "CURRENT_RECORD_IND", "RECORD_EFFECTIVE_DATE", "RECORD_END_DATE", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")

