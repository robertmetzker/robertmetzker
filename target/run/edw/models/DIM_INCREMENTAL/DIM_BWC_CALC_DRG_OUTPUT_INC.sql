
        
        
    

    

    merge into DEV_EDW_32600145.DIM_INCREMENTAL.DIM_BWC_CALC_DRG_OUTPUT_INC as DBT_INTERNAL_DEST
        using DEV_EDW_32600145.DIM_INCREMENTAL.DIM_BWC_CALC_DRG_OUTPUT_INC__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.BWC_CALC_DRG_OUTPUT_HKEY = DBT_INTERNAL_DEST.BWC_CALC_DRG_OUTPUT_HKEY
        

    
    when matched then update set
        "BWC_CALC_DRG_OUTPUT_HKEY" = DBT_INTERNAL_SOURCE."BWC_CALC_DRG_OUTPUT_HKEY","UNIQUE_ID_KEY" = DBT_INTERNAL_SOURCE."UNIQUE_ID_KEY","MEDICAL_INVOICE_NUMBER" = DBT_INTERNAL_SOURCE."MEDICAL_INVOICE_NUMBER","DATE_SENT" = DBT_INTERNAL_SOURCE."DATE_SENT","DIAGNOSIS_RELATED_GROUP_CODE" = DBT_INTERNAL_SOURCE."DIAGNOSIS_RELATED_GROUP_CODE","DIAGNOSIS_RELATED_GROUP_TITLE" = DBT_INTERNAL_SOURCE."DIAGNOSIS_RELATED_GROUP_TITLE","MAJOR_TITLE" = DBT_INTERNAL_SOURCE."MAJOR_TITLE","MAJOR_CATEGORY" = DBT_INTERNAL_SOURCE."MAJOR_CATEGORY","DIAGNOSIS_CODE_1" = DBT_INTERNAL_SOURCE."DIAGNOSIS_CODE_1","DIAGNOSIS_DESC_1" = DBT_INTERNAL_SOURCE."DIAGNOSIS_DESC_1","DIAGNOSIS_CODE_2" = DBT_INTERNAL_SOURCE."DIAGNOSIS_CODE_2","DIAGNOSIS_DESC_2" = DBT_INTERNAL_SOURCE."DIAGNOSIS_DESC_2","DIAGNOSIS_CODE_3" = DBT_INTERNAL_SOURCE."DIAGNOSIS_CODE_3","DIAGNOSIS_DESC_3" = DBT_INTERNAL_SOURCE."DIAGNOSIS_DESC_3","DIAGNOSIS_CODE_4" = DBT_INTERNAL_SOURCE."DIAGNOSIS_CODE_4","DIAGNOSIS_DESC_4" = DBT_INTERNAL_SOURCE."DIAGNOSIS_DESC_4","DIAGNOSIS_CODE_5" = DBT_INTERNAL_SOURCE."DIAGNOSIS_CODE_5","DIAGNOSIS_DESC_5" = DBT_INTERNAL_SOURCE."DIAGNOSIS_DESC_5","OR_PROCEDURE_CODE_1" = DBT_INTERNAL_SOURCE."OR_PROCEDURE_CODE_1","OR_PROCEDURE_DESC_1" = DBT_INTERNAL_SOURCE."OR_PROCEDURE_DESC_1","OR_PROCEDURE_CODE_2" = DBT_INTERNAL_SOURCE."OR_PROCEDURE_CODE_2","OR_PROCEDURE_DESC_2" = DBT_INTERNAL_SOURCE."OR_PROCEDURE_DESC_2","OR_PROCEDURE_CODE_3" = DBT_INTERNAL_SOURCE."OR_PROCEDURE_CODE_3","OR_PROCEDURE_DESC_3" = DBT_INTERNAL_SOURCE."OR_PROCEDURE_DESC_3","OR_PROCEDURE_CODE_4" = DBT_INTERNAL_SOURCE."OR_PROCEDURE_CODE_4","OR_PROCEDURE_DESC_4" = DBT_INTERNAL_SOURCE."OR_PROCEDURE_DESC_4","OR_PROCEDURE_CODE_5" = DBT_INTERNAL_SOURCE."OR_PROCEDURE_CODE_5","OR_PROCEDURE_DESC_5" = DBT_INTERNAL_SOURCE."OR_PROCEDURE_DESC_5","NON_OR_PROCEDURE_CODE_1" = DBT_INTERNAL_SOURCE."NON_OR_PROCEDURE_CODE_1","NON_OR_PROCEDURE_DESC_1" = DBT_INTERNAL_SOURCE."NON_OR_PROCEDURE_DESC_1","NON_OR_PROCEDURE_CODE_2" = DBT_INTERNAL_SOURCE."NON_OR_PROCEDURE_CODE_2","NON_OR_PROCEDURE_DESC_2" = DBT_INTERNAL_SOURCE."NON_OR_PROCEDURE_DESC_2","NON_OR_PROCEDURE_CODE_3" = DBT_INTERNAL_SOURCE."NON_OR_PROCEDURE_CODE_3","NON_OR_PROCEDURE_DESC_3" = DBT_INTERNAL_SOURCE."NON_OR_PROCEDURE_DESC_3","NON_OR_PROCEDURE_CODE_4" = DBT_INTERNAL_SOURCE."NON_OR_PROCEDURE_CODE_4","NON_OR_PROCEDURE_DESC_4" = DBT_INTERNAL_SOURCE."NON_OR_PROCEDURE_DESC_4","NON_OR_PROCEDURE_CODE_5" = DBT_INTERNAL_SOURCE."NON_OR_PROCEDURE_CODE_5","NON_OR_PROCEDURE_DESC_5" = DBT_INTERNAL_SOURCE."NON_OR_PROCEDURE_DESC_5","RETURN_CODE" = DBT_INTERNAL_SOURCE."RETURN_CODE","RETURN_MESSAGE" = DBT_INTERNAL_SOURCE."RETURN_MESSAGE","RETURN_EXTENSION" = DBT_INTERNAL_SOURCE."RETURN_EXTENSION","GROUPER_VERSION" = DBT_INTERNAL_SOURCE."GROUPER_VERSION","GROUPER_METHOD" = DBT_INTERNAL_SOURCE."GROUPER_METHOD","PRICER_VERSION" = DBT_INTERNAL_SOURCE."PRICER_VERSION","LOAD_DATETIME" = DBT_INTERNAL_SOURCE."LOAD_DATETIME","PRIMARY_SOURCE_SYSTEM" = DBT_INTERNAL_SOURCE."PRIMARY_SOURCE_SYSTEM"
    

    when not matched then insert
        ("BWC_CALC_DRG_OUTPUT_HKEY", "UNIQUE_ID_KEY", "MEDICAL_INVOICE_NUMBER", "DATE_SENT", "DIAGNOSIS_RELATED_GROUP_CODE", "DIAGNOSIS_RELATED_GROUP_TITLE", "MAJOR_TITLE", "MAJOR_CATEGORY", "DIAGNOSIS_CODE_1", "DIAGNOSIS_DESC_1", "DIAGNOSIS_CODE_2", "DIAGNOSIS_DESC_2", "DIAGNOSIS_CODE_3", "DIAGNOSIS_DESC_3", "DIAGNOSIS_CODE_4", "DIAGNOSIS_DESC_4", "DIAGNOSIS_CODE_5", "DIAGNOSIS_DESC_5", "OR_PROCEDURE_CODE_1", "OR_PROCEDURE_DESC_1", "OR_PROCEDURE_CODE_2", "OR_PROCEDURE_DESC_2", "OR_PROCEDURE_CODE_3", "OR_PROCEDURE_DESC_3", "OR_PROCEDURE_CODE_4", "OR_PROCEDURE_DESC_4", "OR_PROCEDURE_CODE_5", "OR_PROCEDURE_DESC_5", "NON_OR_PROCEDURE_CODE_1", "NON_OR_PROCEDURE_DESC_1", "NON_OR_PROCEDURE_CODE_2", "NON_OR_PROCEDURE_DESC_2", "NON_OR_PROCEDURE_CODE_3", "NON_OR_PROCEDURE_DESC_3", "NON_OR_PROCEDURE_CODE_4", "NON_OR_PROCEDURE_DESC_4", "NON_OR_PROCEDURE_CODE_5", "NON_OR_PROCEDURE_DESC_5", "RETURN_CODE", "RETURN_MESSAGE", "RETURN_EXTENSION", "GROUPER_VERSION", "GROUPER_METHOD", "PRICER_VERSION", "LOAD_DATETIME", "PRIMARY_SOURCE_SYSTEM")
    values
        ("BWC_CALC_DRG_OUTPUT_HKEY", "UNIQUE_ID_KEY", "MEDICAL_INVOICE_NUMBER", "DATE_SENT", "DIAGNOSIS_RELATED_GROUP_CODE", "DIAGNOSIS_RELATED_GROUP_TITLE", "MAJOR_TITLE", "MAJOR_CATEGORY", "DIAGNOSIS_CODE_1", "DIAGNOSIS_DESC_1", "DIAGNOSIS_CODE_2", "DIAGNOSIS_DESC_2", "DIAGNOSIS_CODE_3", "DIAGNOSIS_DESC_3", "DIAGNOSIS_CODE_4", "DIAGNOSIS_DESC_4", "DIAGNOSIS_CODE_5", "DIAGNOSIS_DESC_5", "OR_PROCEDURE_CODE_1", "OR_PROCEDURE_DESC_1", "OR_PROCEDURE_CODE_2", "OR_PROCEDURE_DESC_2", "OR_PROCEDURE_CODE_3", "OR_PROCEDURE_DESC_3", "OR_PROCEDURE_CODE_4", "OR_PROCEDURE_DESC_4", "OR_PROCEDURE_CODE_5", "OR_PROCEDURE_DESC_5", "NON_OR_PROCEDURE_CODE_1", "NON_OR_PROCEDURE_DESC_1", "NON_OR_PROCEDURE_CODE_2", "NON_OR_PROCEDURE_DESC_2", "NON_OR_PROCEDURE_CODE_3", "NON_OR_PROCEDURE_DESC_3", "NON_OR_PROCEDURE_CODE_4", "NON_OR_PROCEDURE_DESC_4", "NON_OR_PROCEDURE_CODE_5", "NON_OR_PROCEDURE_DESC_5", "RETURN_CODE", "RETURN_MESSAGE", "RETURN_EXTENSION", "GROUPER_VERSION", "GROUPER_METHOD", "PRICER_VERSION", "LOAD_DATETIME", "PRIMARY_SOURCE_SYSTEM")
