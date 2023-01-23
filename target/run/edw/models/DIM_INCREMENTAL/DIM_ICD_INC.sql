
        
        
    

    

    merge into DEV_EDW_32600145.DIM_INCREMENTAL.DIM_ICD_INC as DBT_INTERNAL_DEST
        using DEV_EDW_32600145.DIM_INCREMENTAL.DIM_ICD_INC__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.ICD_HKEY = DBT_INTERNAL_DEST.ICD_HKEY
        

    
    when matched then update set
        "ICD_HKEY" = DBT_INTERNAL_SOURCE."ICD_HKEY","UNIQUE_ID_KEY" = DBT_INTERNAL_SOURCE."UNIQUE_ID_KEY","ICD_CODE" = DBT_INTERNAL_SOURCE."ICD_CODE","ICD_CODE_SHORT_DESC" = DBT_INTERNAL_SOURCE."ICD_CODE_SHORT_DESC","ICD_CODE_LONG_DESC" = DBT_INTERNAL_SOURCE."ICD_CODE_LONG_DESC","ICD_CODE_STATUS_CODE" = DBT_INTERNAL_SOURCE."ICD_CODE_STATUS_CODE","ICD_CODE_STATUS_DESC" = DBT_INTERNAL_SOURCE."ICD_CODE_STATUS_DESC","ICD_CODE_STATUS_EFFECTIVE_DATE" = DBT_INTERNAL_SOURCE."ICD_CODE_STATUS_EFFECTIVE_DATE","ICD_CODE_STATUS_END_DATE" = DBT_INTERNAL_SOURCE."ICD_CODE_STATUS_END_DATE","ICD_CODE_VERSION_NUMBER" = DBT_INTERNAL_SOURCE."ICD_CODE_VERSION_NUMBER","GENDER_SPECIFIC_CODE" = DBT_INTERNAL_SOURCE."GENDER_SPECIFIC_CODE","GENDER_SPECIFIC_DESC" = DBT_INTERNAL_SOURCE."GENDER_SPECIFIC_DESC","AUTHORIZED_BILL_USE_IND" = DBT_INTERNAL_SOURCE."AUTHORIZED_BILL_USE_IND","AUTHORIZED_CLAIM_USE_IND" = DBT_INTERNAL_SOURCE."AUTHORIZED_CLAIM_USE_IND","CATASTROPHIC_ICD_IND" = DBT_INTERNAL_SOURCE."CATASTROPHIC_ICD_IND","CMS_PROHIBITED_IND" = DBT_INTERNAL_SOURCE."CMS_PROHIBITED_IND","ASSIGN_ICD_TO_FIELD_IND" = DBT_INTERNAL_SOURCE."ASSIGN_ICD_TO_FIELD_IND","ICD_ACP_ELIGIBILE_IND" = DBT_INTERNAL_SOURCE."ICD_ACP_ELIGIBILE_IND","ENCODER_VALIDATION_REQUIRED_IND" = DBT_INTERNAL_SOURCE."ENCODER_VALIDATION_REQUIRED_IND","ICD_PRIVACY_IND" = DBT_INTERNAL_SOURCE."ICD_PRIVACY_IND","ICD_STATUTORY_OCCUPATIONAL_DISEASE_IND" = DBT_INTERNAL_SOURCE."ICD_STATUTORY_OCCUPATIONAL_DISEASE_IND","ICD_LOCATION_REQUIRED_IND" = DBT_INTERNAL_SOURCE."ICD_LOCATION_REQUIRED_IND","ICD_PRIOR_AUTHORIZATION_REQUIRED_IND" = DBT_INTERNAL_SOURCE."ICD_PRIOR_AUTHORIZATION_REQUIRED_IND","ICD_POTENTIAL_SEVERE_COMPLICATIONS_IND" = DBT_INTERNAL_SOURCE."ICD_POTENTIAL_SEVERE_COMPLICATIONS_IND","ICD_PSYCHOLOGICAL_IND" = DBT_INTERNAL_SOURCE."ICD_PSYCHOLOGICAL_IND","ICD_SECURITY_AUTHORIZATION_IND" = DBT_INTERNAL_SOURCE."ICD_SECURITY_AUTHORIZATION_IND","ICD_SITE_REQUIRED_IND" = DBT_INTERNAL_SOURCE."ICD_SITE_REQUIRED_IND","ICD_SUBROGATION_REVIEW_IND" = DBT_INTERNAL_SOURCE."ICD_SUBROGATION_REVIEW_IND","EM_REFERRAL_CODE" = DBT_INTERNAL_SOURCE."EM_REFERRAL_CODE","EM_REFERRAL_DESC" = DBT_INTERNAL_SOURCE."EM_REFERRAL_DESC","ICD_CODE_CHAPTER_NUMBER" = DBT_INTERNAL_SOURCE."ICD_CODE_CHAPTER_NUMBER","ICD_CODE_CHAPTER_NUMBER_SHORT_DESC" = DBT_INTERNAL_SOURCE."ICD_CODE_CHAPTER_NUMBER_SHORT_DESC","ICD_CODE_CHAPTER_NUMBER_LONG_DESC" = DBT_INTERNAL_SOURCE."ICD_CODE_CHAPTER_NUMBER_LONG_DESC","BWC_INJURY_CLASS_DESC" = DBT_INTERNAL_SOURCE."BWC_INJURY_CLASS_DESC","BWC_SPECIFIC_BODY_PART_DESC" = DBT_INTERNAL_SOURCE."BWC_SPECIFIC_BODY_PART_DESC","BWC_GENERIC_BODY_PART_DESC" = DBT_INTERNAL_SOURCE."BWC_GENERIC_BODY_PART_DESC","CURRENT_RECORD_IND" = DBT_INTERNAL_SOURCE."CURRENT_RECORD_IND","RECORD_EFFECTIVE_DATE" = DBT_INTERNAL_SOURCE."RECORD_EFFECTIVE_DATE","RECORD_END_DATE" = DBT_INTERNAL_SOURCE."RECORD_END_DATE","LOAD_DATETIME" = DBT_INTERNAL_SOURCE."LOAD_DATETIME","UPDATE_DATETIME" = DBT_INTERNAL_SOURCE."UPDATE_DATETIME","PRIMARY_SOURCE_SYSTEM" = DBT_INTERNAL_SOURCE."PRIMARY_SOURCE_SYSTEM"
    

    when not matched then insert
        ("ICD_HKEY", "UNIQUE_ID_KEY", "ICD_CODE", "ICD_CODE_SHORT_DESC", "ICD_CODE_LONG_DESC", "ICD_CODE_STATUS_CODE", "ICD_CODE_STATUS_DESC", "ICD_CODE_STATUS_EFFECTIVE_DATE", "ICD_CODE_STATUS_END_DATE", "ICD_CODE_VERSION_NUMBER", "GENDER_SPECIFIC_CODE", "GENDER_SPECIFIC_DESC", "AUTHORIZED_BILL_USE_IND", "AUTHORIZED_CLAIM_USE_IND", "CATASTROPHIC_ICD_IND", "CMS_PROHIBITED_IND", "ASSIGN_ICD_TO_FIELD_IND", "ICD_ACP_ELIGIBILE_IND", "ENCODER_VALIDATION_REQUIRED_IND", "ICD_PRIVACY_IND", "ICD_STATUTORY_OCCUPATIONAL_DISEASE_IND", "ICD_LOCATION_REQUIRED_IND", "ICD_PRIOR_AUTHORIZATION_REQUIRED_IND", "ICD_POTENTIAL_SEVERE_COMPLICATIONS_IND", "ICD_PSYCHOLOGICAL_IND", "ICD_SECURITY_AUTHORIZATION_IND", "ICD_SITE_REQUIRED_IND", "ICD_SUBROGATION_REVIEW_IND", "EM_REFERRAL_CODE", "EM_REFERRAL_DESC", "ICD_CODE_CHAPTER_NUMBER", "ICD_CODE_CHAPTER_NUMBER_SHORT_DESC", "ICD_CODE_CHAPTER_NUMBER_LONG_DESC", "BWC_INJURY_CLASS_DESC", "BWC_SPECIFIC_BODY_PART_DESC", "BWC_GENERIC_BODY_PART_DESC", "CURRENT_RECORD_IND", "RECORD_EFFECTIVE_DATE", "RECORD_END_DATE", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
    values
        ("ICD_HKEY", "UNIQUE_ID_KEY", "ICD_CODE", "ICD_CODE_SHORT_DESC", "ICD_CODE_LONG_DESC", "ICD_CODE_STATUS_CODE", "ICD_CODE_STATUS_DESC", "ICD_CODE_STATUS_EFFECTIVE_DATE", "ICD_CODE_STATUS_END_DATE", "ICD_CODE_VERSION_NUMBER", "GENDER_SPECIFIC_CODE", "GENDER_SPECIFIC_DESC", "AUTHORIZED_BILL_USE_IND", "AUTHORIZED_CLAIM_USE_IND", "CATASTROPHIC_ICD_IND", "CMS_PROHIBITED_IND", "ASSIGN_ICD_TO_FIELD_IND", "ICD_ACP_ELIGIBILE_IND", "ENCODER_VALIDATION_REQUIRED_IND", "ICD_PRIVACY_IND", "ICD_STATUTORY_OCCUPATIONAL_DISEASE_IND", "ICD_LOCATION_REQUIRED_IND", "ICD_PRIOR_AUTHORIZATION_REQUIRED_IND", "ICD_POTENTIAL_SEVERE_COMPLICATIONS_IND", "ICD_PSYCHOLOGICAL_IND", "ICD_SECURITY_AUTHORIZATION_IND", "ICD_SITE_REQUIRED_IND", "ICD_SUBROGATION_REVIEW_IND", "EM_REFERRAL_CODE", "EM_REFERRAL_DESC", "ICD_CODE_CHAPTER_NUMBER", "ICD_CODE_CHAPTER_NUMBER_SHORT_DESC", "ICD_CODE_CHAPTER_NUMBER_LONG_DESC", "BWC_INJURY_CLASS_DESC", "BWC_SPECIFIC_BODY_PART_DESC", "BWC_GENERIC_BODY_PART_DESC", "CURRENT_RECORD_IND", "RECORD_EFFECTIVE_DATE", "RECORD_END_DATE", "LOAD_DATETIME", "UPDATE_DATETIME", "PRIMARY_SOURCE_SYSTEM")
