repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING\DIM_CLAIM_DETAIL_SCDALL_STEP2.sql:
   5:  {{dbt_utils.generate_surrogate_key ( ['FILING_SOURCE_DESC','FILING_MEDIA_DESC','NATURE_OF_INJURY_CATEGORY', 'NATURE_OF_INJURY_TYPE','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_IND','SB223_IND','EMPLOYER_PREMISES_IND','CATASTROPHIC_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC' ])}} AS UNIQUE_ID_KEY 
  24:  {{dbt_utils.generate_surrogate_key ( ['FILING_SOURCE_DESC','FILING_MEDIA_DESC','NATURE_OF_INJURY_CATEGORY', 'NATURE_OF_INJURY_TYPE','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_IND','SB223_IND','EMPLOYER_PREMISES_IND','CATASTROPHIC_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC' ])}} AS UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING\DIM_COVERED_INDIVIDUAL_CUSTOMER_SCDALL_STEP2.sql:
  5:  {{dbt_utils.generate_surrogate_key ( ['COVERED_INDIVIDUAL_CUSTOMER_NUMBER'])}} as COVERED_INDIVIDUAL_CUSTOMER_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING\DIM_EXAM_REQUEST_SCDALL_STEP2.sql:
  5: {{dbt_utils.generate_surrogate_key ( ['EXAM_TYPE_CODE','CASE_PROFILE_CATEGORY_CODE','EXAM_REQUESTOR_TYPE_CODE', 'PHYSICIAN_SPECIALTY_NEEDED_CODE','SECOND_CHOICE_PHYSICIAN_SPECIALTY_CODE','ADDENDUM_REQUEST_TYPE_CODE','LANGUAGE_TYPE_CODE', 'EXAM_RESCHEDULE_STATUS_REASON_CODE','REFERRED_TO'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING\DIM_FUNCTIONAL_ROLE_ORGANIZATION_UNIT_SCDALL_STEP2.sql:
  8: {{ dbt_utils.generate_surrogate_key ( [ 'ORGANIZATIONAL_UNIT_NAME','USER_FUNCTIONAL_ROLE_CODE'] ) }}  as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING\DIM_INDEMNITY_PLAN_SCHEDULE_DETAIL_SCDALL_STEP2.sql:
  7: {{ dbt_utils.generate_surrogate_key ( [ 'INDM_FREQ_TYP_CODE','INDM_RSN_TYP_CODE','INDM_SCH_DTL_AMT_TYP_CODE','INDM_SCH_DTL_STS_TYP_CODE','WARRANT_STATUS_CODE','INDM_SCH_DTL_FNL_PAY_IND','INDM_SCH_AUTO_PAY_IND','INDM_PAY_RECALC_IND','INDM_SCH_DTL_AMT_PRI_IND','INDM_SCH_DTL_AMT_MAILTO_IND','INDM_SCH_DTL_AMT_RMND_IND','OVR_PYMNT_BAL_IND','IP_VOID_IND','ISS_VOID_IND','ISD_VOID_IND', 'ISDA_VOID_IND' ] ) }} AS UNIQUE_ID_KEY  

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING\DIM_IW_EXAM_SCHEDULE_GUIDE_SCDALL_STEP2.sql:
  5: {{dbt_utils.generate_surrogate_key ( ['MONDAY_AVAILABILITY_IND','TUESDAY_AVAILABILITY_IND','WEDNESDAY_AVAILABILITY_IND', 'THURSDAY_AVAILABILITY_IND','FRIDAY_AVAILABILITY_IND','SATURDAY_AVAILABILITY_IND','SUNDAY_AVAILABILITY_IND', 'INTERPRETER_NEEDED_IND','GREATER_THAN_45_MILES_IND','TRAVEL_REIMBURSEMENT_IND','ADDITIONAL_TESTING_IND', 'ADDENDUM_REQUESTED_IND','RESULT_SUSPENDED_IND','NO_SHOW_IND','RESCHEDULE_IND'])}} AS UNIQUE_ID_KEY    

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING\DIM_PBM_PRICING_METHOD_SCDALL_STEP2.sql:
  5: {{dbt_utils.generate_surrogate_key ( ['PRICE_TYPE_CODE','PBM_PRICING_SOURCE_CODE'])}} as PBM_PRICING_METHOD_HKEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_ACTIVITY_DETAIL.sql:
  22: {{ dbt_utils.generate_surrogate_key ( ['ACTIVITY_DETAIL_DESC'] ) }} As ACTIVITY_DETAIL_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_ADMISSION.sql:
  55:  {{dbt_utils.generate_surrogate_key ( ['HOSPITAL_ADMISSION_TYPE_CODE','MEDICAL_INVOICE_SOURCE_OF_ADMISSION_CODE','MEDICAL_INVOICE_HOSPITAL_DISCHARGE_STATUS_CODE', 'MEDICAL_INVOICE_HOSPITAL_BILL_TYPE_CODE'])}} AS ADMISSION_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_ALLOWANCE_REQUEST_PROFILE.sql:
  55: DISTINCT {{dbt_utils.generate_surrogate_key ( ['ALLOWANCE_REQUEST_FORM_CODE','ALLOWANCE_REQUEST_REVIEW_TYPE','SUBSTANTIAL_AGGREVATION_IND','PSYCH_ICD_IND','CLAIM_REACTIVATION_IND','ALLOWANCE_REQUEST_DECISION','DECISION_MAKER_FUNCTIONAL_ROLE'])}} AS ALLOWANCE_REQUEST_PROFILE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_AMBULATORY_PAYMENT_CLASSIFICATION.sql:
  43: {{dbt_utils.generate_surrogate_key ( ['APC_CODE','EFFECTIVE_DATE'])}} AS APC_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_BENEFIT_TYPE.sql:
  13: {{dbt_utils.generate_surrogate_key ( ['BENEFIT_TYPE_CODE','JURISDICTION_TYPE_CODE','BENEFIT_REPORTING_TYPE_DESC','INJURY_TYPE_CODE'])}} AS UNIQUE_ID_KEY,
  58: {{dbt_utils.generate_surrogate_key ( ['BENEFIT_TYPE_CODE','JURISDICTION_TYPE_CODE','BENEFIT_REPORTING_TYPE_DESC','SCHEDULE_AWARD_INJURY_TYPE_CODE'])}} AS BENEFIT_TYPE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_BWC_CALC_DRG_OUTPUT.sql:
  226: {{dbt_utils.generate_surrogate_key ( ['MEDICAL_INVOICE_NUMBER'])}} AS BWC_CALC_DRG_OUTPUT_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CASE_TYPE.sql:
  70: {{dbt_utils.generate_surrogate_key (['CONTEXT_TYPE_CODE', 'CASE_CATEGORY_TYPE_CODE', 'CASE_TYPE_CODE', 'CASE_PRIORITY_TYPE_CODE', 'CASE_RESOLUTION_TYPE_CODE'])}} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CATASTROPHIC_INJURY_TYPE.sql:
  14: {{dbt_utils.generate_surrogate_key ( ['CATASTROPHIC_INJURY_TYPE_CODE'])}} AS UNIQUE_ID_KEY,
  29: {{dbt_utils.generate_surrogate_key ( ['CATASTROPHIC_INJURY_TYPE_CODE'])}} AS CATASTROPHIC_INJURY_TYPE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CLAIM_DETAIL.sql:
  95: {{dbt_utils.generate_surrogate_key ( ['FILING_SOURCE_DESC','FILING_MEDIA_DESC','NATURE_OF_INJURY_CATEGORY', 'NATURE_OF_INJURY_TYPE','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_IND','SB223_IND','EMPLOYER_PREMISES_IND','CATASTROPHIC_IND','EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC','EMPLOYER_PAID_PROGRAM_TYPE','EMPLOYER_PAID_PROGRAM_REASON' ])}} AS CLAIM_DETAIL_HKEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CLAIM_ICD_SPECIFIC_DESC.sql:
  27: {{ dbt_utils.generate_surrogate_key ( ['CLAIM_ICD_SPECIFIC_DESC'] ) }} As  CLAIM_ICD_SPECIFIC_DESC_HKEY
  28: {{ dbt_utils.generate_surrogate_key ( ['CLAIM_ICD_SPECIFIC_DESC'] ) }} As  UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CLAIM_INVESTIGATION.sql:
  27: Distinct {{dbt_utils.generate_surrogate_key ( ['CLAIM_ACP_STATUS_IND','ACP_MANUAL_INTERVENTION_IND','JURISDICTION_TYPE_CODE'])}} AS CLAIM_INVESTIGATION_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CLAIM_PAYMENT_CATEGORY.sql:
  35: {{dbt_utils.generate_surrogate_key ( ['CLAIM_PAYMENT_CATEGORY_DESC'])}} AS CLAIM_PAYMENT_CATEGORY_HKEY
  36: {{dbt_utils.generate_surrogate_key ( ['CLAIM_PAYMENT_CATEGORY_DESC'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CLAIM_POLICY_ASSIGNMENT_DETAIL.sql:
  19: {{dbt_utils.generate_surrogate_key (['CRNT_PLCY_IND', 'CTL_ELEM_SUB_TYP_CD'])}}  AS                        UNIQUE_ID_KEY
  42: {{dbt_utils.generate_surrogate_key ( ['CURRENT_CLAIM_POLICY_IND','POLICY_TYPE_CODE'])}} as CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CLAIM_STATUS.sql:
  49: {{dbt_utils.generate_surrogate_key ( ['CLAIM_STATE_CODE','CLAIM_STATUS_CODE', 'CLAIM_STATUS_REASON_CODE'])}} AS CLAIM_STATUS_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CLAIM_TYPE_STATUS.sql:
  80: {{dbt_utils.generate_surrogate_key ( ['CLAIM_TYPE_CODE','CLAIM_TYPE_CHANGE_OVER_IND','CLAIM_STATE_CODE','CLAIM_STATUS_CODE','CLAIM_STATUS_REASON_CODE'] ) }} AS CLAIM_TYPE_STATUS_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CLAIM.sql:
  187: {{dbt_utils.generate_surrogate_key ( ['CLAIM_NUMBER']) }} as CLAIM_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_CPT.sql:
  64:  {{dbt_utils.generate_surrogate_key ( ['PROCEDURE_CODE', 'EFFECTIVE_DATE'] ) }} as CPT_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_DIAGNOSIS_RELATED_GROUP.sql:
  52: {{dbt_utils.generate_surrogate_key ( ['DIAGNOSIS_RELATED_GROUP_CODE','EFFECTIVE_DATE']) }} AS DRG_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_DOCUMENT_TYPE.sql:
  15: last_value( {{dbt_utils.generate_surrogate_key ( ['DOCUMENT_TYPE_REFERENCE_NUMBER','DOCUMENT_VERSION_NUMBER'])}}) over 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_DRUG_PRICING_TYPE.sql:
  26: {{dbt_utils.generate_surrogate_key ( ['PRICE_TYPE_CODE'])}} as PRICING_TYPE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_EDIT_EOB_ENTRY.sql:
  38: {{dbt_utils.generate_surrogate_key ( ['SOURCE_CODE','ADJUDICATION_PHASE','DISPOSITION_CODE'])}} as EDIT_EOB_ENTRY_HKEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_EDIT.sql:
  49: {{dbt_utils.generate_surrogate_key ( ['EDIT_CODE', 'EFFECTIVE_DATE'])}} as EDIT_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_EMPLOYER.sql:
  147:  {{dbt_utils.generate_surrogate_key ( ['CUSTOMER_NUMBER','EFFECTIVE_DATE'])}} as EMPLOYER_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_EOB.sql:
  49: {{dbt_utils.generate_surrogate_key ( ['EOB_CODE', 'EFFECTIVE_DATE'])}} as EOB_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_EXAM_REQUEST.sql:
  102: {{dbt_utils.generate_surrogate_key ( ['EXAM_TYPE_CODE','CASE_PROFILE_CATEGORY_CODE','EXAM_REQUESTOR_TYPE_CODE', 'PHYSICIAN_SPECIALTY_NEEDED_CODE','SECOND_CHOICE_PHYSICIAN_SPECIALTY_CODE','ADDENDUM_REQUEST_TYPE_CODE','LANGUAGE_TYPE_CODE', 'EXAM_RESCHEDULE_STATUS_REASON_CODE','REFERRED_TO'])}} AS EXAM_REQUEST_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_EXAM_SCHEDULE.sql:
  84:  {{ dbt_utils.generate_surrogate_key ( ['MONDAY_AVAILABILITY_IND','TUESDAY_AVAILABILITY_IND','WEDNESDAY_AVAILABILITY_IND','THURSDAY_AVAILABILITY_IND','FRIDAY_AVAILABILITY_IND','SATURDAY_AVAILABILITY_IND','SUNDAY_AVAILABILITY_IND','INTERPRETER_NEEDED_IND','GREATER_THAN_45_MILES_IND','TRAVEL_REIMBURSEMENT_IND','ADDITIONAL_TESTING_IND','ADDENDUM_REQUESTED_IND','RESULT_SUSPENDED_IND'] ) }} As EXAM_SCHEDULE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_FINANCIAL_TRANSACTION_STATUS.sql:
  31:  {{dbt_utils.generate_surrogate_key ( ['FINANCIAL_TRANSACTION_STATUS_CODE'])}} as FINANCIAL_TRANSACTION_STATUS_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_FINANCIAL_TRANSACTION_TYPE.sql:
  44:  {{dbt_utils.generate_surrogate_key ( ['FINANCIAL_TRANSACTION_TYPE_ID'])}} as FINANCIAL_TRANSACTION_TYPE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_FUNCTIONAL_ROLE_ORGANIZATION_UNIT.sql:
  39: {{ dbt_utils.generate_surrogate_key ( [ 'ORGANIZATIONAL_UNIT_NAME','USER_FUNCTIONAL_ROLE_CODE'] ) }}  as FUNCTIONAL_ROLE_ORGANIZATION_UNIT_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_HOSPITAL_ICD_PROCEDURE.sql:
  51: {{dbt_utils.generate_surrogate_key ( ['ICD_PROCEDURE_CODE', 'EFFECTIVE_DATE'])}} as ICD_PROCEDURE_HKEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_ICD.sql:
  180: {{dbt_utils.generate_surrogate_key ( ['ICD_CODE', 'EFFECTIVE_DATE'])}} as ICD_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_INDIVIDUAL_COVERAGE_TYPE.sql:
  35: {{dbt_utils.generate_surrogate_key ( ['INDIVIDUAL_COVERAGE_TYPE_DESC', 'INDIVIDUAL_COVERAGE_TITLE', 'INDIVIDUAL_COVERAGE_INCLUSION_IND'])}} AS INDIVIDUAL_COVERAGE_TYPE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_INJURED_WORKER.sql:
  153: {{dbt_utils.generate_surrogate_key ( ['CUSTOMER_NUMBER', 'EFFECTIVE_DATE'] ) }} as INJURED_WORKER_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_INVOICE_PROFILE.sql:
  65: {{dbt_utils.generate_surrogate_key ( ['MEDICAL_INVOICE_TYPE_CODE', 'IN_SUBROGATION_IND', 'ADJUSTMENT_TYPE', 'INVOICE_INPUT_METHOD_CODE', 'INVOICE_PAYMENT_CATEGORY', 'INVOICE_FEE_SCHEDULE_DESC', 'PAID_ABOVE_ZERO_IND', 'SUBROGATION_TYPE_DESC'])}}

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_IW_EXAM_SCHEDULE_GUIDE.sql:
  89: {{dbt_utils.generate_surrogate_key ( ['MONDAY_AVAILABILITY_IND','TUESDAY_AVAILABILITY_IND','WEDNESDAY_AVAILABILITY_IND', 'THURSDAY_AVAILABILITY_IND','FRIDAY_AVAILABILITY_IND','SATURDAY_AVAILABILITY_IND','SUNDAY_AVAILABILITY_IND', 'INTERPRETER_NEEDED_IND','GREATER_THAN_45_MILES_IND','TRAVEL_REIMBURSEMENT_IND','ADDITIONAL_TESTING_IND', 'ADDENDUM_REQUESTED_IND','RESULT_SUSPENDED_IND','NO_SHOW_IND','RESCHEDULE_IND'])}} AS IW_EXAM_SCHEDULE_GUIDE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_MANUAL_CLASSIFICATION.sql:
  51: {{dbt_utils.generate_surrogate_key ( ['MANUAL_CLASS_CODE', 'MANUAL_CLASS_SUFFIX_CODE','EFFECTIVE_DATE'] ) }} as MANUAL_CLASS_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_MODIFIER_SEQUENCE.sql:
  65: {{dbt_utils.generate_surrogate_key ( ['MODIFIER_SEQUENCE_1_CODE','MODIFIER_SEQUENCE_2_CODE','MODIFIER_SEQUENCE_3_CODE','MODIFIER_SEQUENCE_4_CODE','MODIFIER_SET_CODE' ])}} as MODIFIER_SEQUENCE_CODE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_NDC.sql:
  240: {{dbt_utils.generate_surrogate_key ( ['NDC_11_CODE','EFFECTIVE_DATE'])}} AS NDC_GPI_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_NETWORK.sql:
  166: {{dbt_utils.generate_surrogate_key ( ['NETWORK_NUMBER', 'EFFECTIVE_DATE'])}} as NETWORK_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_ORGANIZATIONAL_UNIT.sql:
  31: {{dbt_utils.generate_surrogate_key ( ['ORGANIZATIONAL_UNIT_NAME'])}}  AS ORGANIZATION_UNIT_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_OUTPATIENT_GROUPER.sql:
  108: {{dbt_utils.generate_surrogate_key ( ['APC_STATUS_INDICATOR_CODE','OPPS_RETURN_CODE','OPPS_CODE', 'OPPS_PAYMENT_METHOD_CODE','APC_DISCOUNTING_FRACTION_CODE','APC_COMPOSITE_ADJUSTMENT_CODE','APC_PAYMENT_INDICATOR_CODE','APC_PACKAGING_CODE','APC_PAYMENT_ADJUSTMENT_CODE'])}} as GROUPER_HKEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_PAYEE.sql:
  19:  {{dbt_utils.generate_surrogate_key ( ['PAYEE_FULL_NAME'])}} AS PAYEE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_PAYMENT_REQUEST.sql:
  81:  {{dbt_utils.generate_surrogate_key ( ['PAY_MEDIA_PREFERENCE_TYPE_CODE','PAY_REQUEST_TYPE_CODE','PAY_REQUEST_STATE_TYPE_CODE','PAY_REQUEST_STATUS_TYPE_CODE','PAY_REQUEST_STATUS_REASON_TYPE_CODE'])}} AS PAYMENT_REQUEST_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_PLACE_OF_SERVICE.sql:
  40:  {{dbt_utils.generate_surrogate_key ( ['LINE_PLACE_OF_SERVICE_CODE', 'EFFECTIVE_DATE'])}} as PLACE_OF_SERVICE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_POLICY_BILLING.sql:
  59:  {{ dbt_utils.generate_surrogate_key ( ['PAYMENT_PLAN_TYPE_DESC','REPORTING_FREQUENCY_TYPE_DESC','AUDIT_TYPE_DESC','EMPLOYEE_LEASING_TYPE_DESC','POLICY_15K_PROGRAM_IND','ESTIMATED_ZERO_PAYROLL_IND','REPORTED_ZERO_PAYROLL_IND','ESTIMATED_PREMIUM_IND'] ) }} As POLICY_BILLING_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_POLICY_FINANCIAL_TRANSACTION_COMMENT.sql:
  23:  {{ dbt_utils.generate_surrogate_key ( ['FINANCIAL_TRANSACTION_COMMENT_TEXT'] ) }} As FINANCIAL_TRANSACTION_COMMENT_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_POLICY.sql:
  45: {{ dbt_utils.generate_surrogate_key ( ['PAYMENT_PLAN_TYPE_DESC','REPORTING_FREQUENCY_TYPE_DESC','AUDIT_TYPE_DESC','EMPLOYEE_LEASING_TYPE_DESC','POLICY_EMPLOYER_PAID_PROGRAM_IND'] ) }} as POLICY_HKEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_PREMIUM_CALCULATION_TYPE.sql:
  49: {{dbt_utils.generate_surrogate_key ( ['PREMIUM_CALCULATION_TYPE_DESC','CURRENT_PREMIUM_CALCULATION_IND','EXPOSURE_TYPE_DESC','EXPOSURE_AUDIT_TYPE_DESC'])}} AS PREMIUM_CALCULATION_TYPE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_PRESCRIPTION_BILL.sql:
  101: {{dbt_utils.generate_surrogate_key ( ['SERVICE_LEVEL_CODE','PHARMACIST_SERVICE_CODE','SERVICE_REASON_CODE','SERVICE_RESULT_CODE','SUBMITTED_DAW_CODE','SUBMISSION_CLARIFICATION_CODE','PBM_BENEFIT_PLAN_TYPE_DESC','PBM_ORIGINATION_TYPE_DESC','PHARM_SPECIAL_PROGRAM_DESC',

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_PROVIDER.sql:
  313: {{dbt_utils.generate_surrogate_key ( ['PROVIDER_PEACH_NUMBER','EFFECTIVE_DATE'])}} as  PROVIDER_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_RATING_PLAN_TYPE.sql:
  28:  {{dbt_utils.generate_surrogate_key ( ['RATING_PLAN_CODE'])}} AS RATING_PLAN_TYPE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_REPRESENTATIVE.sql:
  118: {{dbt_utils.generate_surrogate_key ( ['CUSTOMER_NUMBER','RECORD_EFFECTIVE_DATE'])}} as REPRESENTATIVE_HKEY, * FROM SCD )

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_REVENUE_CENTER.sql:
  39:  {{dbt_utils.generate_surrogate_key ( ['HOSPITAL_REVENUE_CENTER_CODE','EFFECTIVE_DATE'])}} as REVENUE_CENTER_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STAGING_DIM\DIM_WRITTEN_PREMIUM_ELEMENT.sql:
  46:  {{dbt_utils.generate_surrogate_key ( ['WRITTEN_PREMIUM_ELEMENT_CODE','WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE','WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE'])}} as WRITTEN_PREMIUM_ELEMENT_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_CLAIMS_MART\FACT_ADDITIONAL_ALLOWANCE_REQUEST_TIMELINE.sql:
  51: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'ALLOWANCE_REQUEST_FORM_CODE','ALLOWANCE_REQUEST_REVIEW_TYPE','SUBSTANTIAL_AGGREVATION_IND','PSYCH_ICD_IND','CLAIM_REACTIVATION_IND','ALLOWANCE_REQUEST_DECISION','DECISION_MAKER_FUNCTIONAL_ROLE' ] ) }} 
  55: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CLAIM_ICD_DESC' ] ) }} 
  59: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CLAIM_ICD_STATUS_CODE','CLAIM_ICD_LOCATION_CODE','CLAIM_ICD_SITE_CODE','CLAIM_ICD_PRIMARY_IND','CURRENT_ICD_IND' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_CLAIMS_MART\FACT_CLAIM_CORRESPONDENCE.sql:
  40:  {{ dbt_utils.generate_surrogate_key ( [ 'DOCM_TYP_REF_NO','DOCM_TYP_VER_NO' ] ) }} 
  42:  {{ dbt_utils.generate_surrogate_key ( [ 'DOCM_STT_TYP_CD','DOCM_STS_TYP_CD','DOCM_SYS_GEN_IND','DOCM_TYP_VER_DUP_IND','DOCM_TYP_VER_SYS_GEN_IND','DOCM_TYP_VER_SYS_GEN_ONLY_IND','DOCM_TYP_VER_CANC_PLCY_IND','DOCM_TYP_VER_PRE_PRNT_ENCL_IND','DOCM_TYP_VER_BTCH_PRNT_IND','DOCM_TYP_VER_USR_CAN_DEL_IND','DOCM_TYP_VER_MULTI_RDR_VER_IND','DOCM_TYP_VER_PRNT_DUPLX_IND' ] ) }} 
  70:  {{ dbt_utils.generate_surrogate_key ( [ 'CR_USER_LGN_NM' ] ) }} 
  77:  {{ dbt_utils.generate_surrogate_key ( [ 'UP_USER_LGN_NM' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_CLAIMS_MART\FACT_CLAIM_EXAM_SCHEDULE.sql:
   53:  then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'APP_CNTX_TYP_CD','CASE_CTG_TYP_CD','CASE_TYP_CD','CASE_PRTY_TYP_CD','CASE_RSOL_TYP_CD' ] ) }} 
   57: 	then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CD_EXM_REQS_TYP_CD','CPS_TYP_CD','CPS_TYP_CD_SCND','CD_ADNDM_REQS_TYP_CD','LANG_TYP_CD' ] ) }} 
   61: 	then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CDES_CLMT_AVL_MON_IND','CDES_CLMT_AVL_TUE_IND','CDES_CLMT_AVL_WED_IND','CDES_CLMT_AVL_THU_IND','CDES_CLMT_AVL_FRI_IND','CDES_CLMT_AVL_SAT_IND','CDES_CLMT_AVL_SUN_IND','CDES_ITPRT_NEED_IND','CDES_GRTT_45_IND','CDES_TRVL_REMB_IND','CDES_ADDTNL_TST_IND','CDES_ADNDM_REQS_IND','CDES_RSLT_SUSPD_IND' ] ) }} 
   95: 	then MD5( '99999' ) ELSE {{ dbt_utils .generate_surrogate_key ( [ 'POLICY_TYPE_CODE','PLCY_STS_TYP_CD','PLCY_STS_RSN_TYP_CD','POLICY_ACTIVE_IND' ] ) }} 
   99: 	then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CLM_LOSS_DESC','CLM_CLMT_JOB_TTL' ] ) }} 
  103: 	then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CLM_TYP_CD','CHNG_OVR_IND','CLM_STT_TYP_CD','CLM_STS_TYP_CD','CLM_TRANS_RSN_TYP_CD' ] ) }} 
  107: 	then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'OCCR_SRC_TYP_NM','OCCR_MEDA_TYP_NM','NOI_CTG_TYP_NM','NOI_TYP_NM','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_CLAIM_IND','SB223_IND','EMPLOYER_PREMISES_IND','CLM_CTRPH_INJR_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_CLAIMS_MART\FACT_CLAIM_PROGRESS_SNAPSHOT.sql:
  157: 	then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'INDUSTRY_GROUP_CODE' ] ) }} 
  161: {{ dbt_utils.generate_surrogate_key ( [ 'CLM_ENTRY_USER_LGN_NM' ] ) }} as                         ENTRY_USER_HKEY 
  177: , CASE WHEN CSS_USER_LGN_NM IS NULL THEN {{ dbt_utils.generate_surrogate_key ( [ '99999' ]) }}
  178:        ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CSS_USER_LGN_NM' ]  ) }}    
  180: , CASE WHEN DETERMINATION_USER_LGN_NM IS NULL THEN {{ dbt_utils.generate_surrogate_key ( [ '99999' ]) }}
  181:        ELSE {{ dbt_utils.generate_surrogate_key ( [ 'DETERMINATION_USER_LGN_NM' ]  ) }}    
  184: , CASE WHEN ORG_UNT_NM IS NULL THEN {{ dbt_utils.generate_surrogate_key ( [ '99999' ]) }}
  185:        ELSE {{ dbt_utils.generate_surrogate_key ( [ 'ORG_UNT_NM' ]  ) }}   
  205:  {{dbt_utils.generate_surrogate_key (['POLICY_TYPE_CODE', 'PLCY_STS_TYP_CD', 'PLCY_STS_RSN_TYP_CD', 'POLICY_ACTIVE_IND'])}} 
  223: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CURRENT_CORESUITE_CLAIM_TYPE_CODE','CLAIM_TYPE_CHNG_OVR_IND','CLAIM_STATE_CODE','CLAIM_STATUS_CODE','CLAIM_STATUS_REASON_CODE' ] ) }} 
  225:  {{ dbt_utils.generate_surrogate_key ( [ 'FILING_SOURCE_DESC','FILING_MEDIA_DESC','NATURE_OF_INJURY_CATEGORY','NATURE_OF_INJURY_TYPE','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_IND','SB223_IND','EMPLOYER_PREMISES_IND','CATASTROPHIC_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC' ] ) }} 
  228: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'ACCIDENT_DESCRIPTION_TEXT','IW_JOB_TITLE' ] ) }} 
  243: , CASE WHEN CATASTROPHIC_INJURY_TYPE_CODE IS NULL THEN {{ dbt_utils.generate_surrogate_key ( [ '99999' ]) }}
  244:        ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CATASTROPHIC_INJURY_TYPE_CODE' ]  ) }}    END 
  361:  {{ dbt_utils.generate_surrogate_key ( [ 'CLAIM_ACP_STATUS_IND','ACP_MANUAL_INTERVENTION_IND','JURISDICTION_TYPE_CODE' ] ) }} 
  452: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'DISABILITY_TYPE_CODE','DISABILITY_REASON_TYPE_CODE','DISABILITY_MEDICAL_STATUS_TYPE_CODE','DISABILITY_WORK_STATUS_TYPE_CODE','CURRENT_DISABILITY_STATUS_IND' ] ) }} 
  752: {{dbt_utils.generate_surrogate_key ( ['99999'])}} AS FILING_PARTY_HKEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_CLAIMS_MART\FACT_DISABILITY_TRACKING.sql:
   24: {{ dbt_utils.generate_surrogate_key ( [ 'CLAIM_NUMBER' ] ) }} 
   29: {{dbt_utils.generate_surrogate_key (['DISABILITY_TYPE_CODE','DISABILITY_REASON_TYPE_CODE','DISABILITY_MEDICAL_STATUS_TYPE_CODE','DISABILITY_WORK_STATUS_TYPE_CODE', 'CURRENT_DISABILITY_STATUS_IND'])}} 
   57: {{ dbt_utils.generate_surrogate_key ( [ 'CREATE_USER_LOGIN_NAME' ] ) }}  
   59: {{ dbt_utils.generate_surrogate_key ( [ 'UPDATE_USER_LOGIN_NAME' ] ) }}                             
  113: coalesce( CREATE_USER_HKEY, {{dbt_utils.generate_surrogate_key ( ['99999'])}} ) AS CREATE_USER_HKEY
  114: coalesce( UPDATE_USER_HKEY, {{dbt_utils.generate_surrogate_key ( ['99999'])}} ) AS UPDATE_USER_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_CLAIMS_MART\FACT_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT.sql:
   71: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'BENEFIT_TYPE_CODE','JURISDICTION_TYPE_CODE','BENEFIT_REPORTING_TYPE_DESC', 'INJURY_TYPE_CODE' ] ) }} 
   75: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'PTCP_TYP_CODE','CLM_PTCP_PRI_IND' ] ) }} 
   91: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'INDM_FREQ_TYP_CODE','INDM_RSN_TYP_CODE','INDM_SCH_DTL_AMT_TYP_CODE','INDM_SCH_DTL_STS_TYP_CODE','WARRANT_STATUS_CODE','INDM_SCH_DTL_FNL_PAY_IND','INDM_SCH_AUTO_PAY_IND','INDM_PAY_RECALC_IND','INDM_SCH_DTL_AMT_PRI_IND','INDM_SCH_DTL_AMT_MAILTO_IND','INDM_SCH_DTL_AMT_RMND_IND','OVR_PYMNT_BAL_IND','IP_VOID_IND','ISS_VOID_IND','ISD_VOID_IND','ISDA_VOID_IND' ] ) }} 
   95: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'INDMN_PLAN_CREATE_USER_LGN_NAME' ] ) }} 
   99: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CLAIM_AUTH_USER_LGN_NAME' ] ) }} 
  133: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'PAY_MEDA_PREF_TYP_CODE','PAY_REQS_TYP_CODE','PAY_REQS_STT_TYP_CODE','PAY_REQS_STS_TYP_CODE','PAY_REQS_STS_RSN_TYP_CODE' ] ) }} 
  217: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CURRENT_CORESUITE_CLAIM_TYPE_CODE','CLAIM_TYPE_CHNG_OVR_IND','CLAIM_STATE_CODE','CLAIM_STATUS_CODE','CLAIM_STATUS_REASON_CODE' ] ) }} 
  221: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'FILING_SOURCE_DESC','FILING_MEDIA_DESC','NATURE_OF_INJURY_CATEGORY','NATURE_OF_INJURY_TYPE','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_IND','SB223_IND','EMPLOYER_PREMISES_IND','CATASTROPHIC_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC' ] ) }} 
  247: THEN MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CSS_USER_LGN_NM' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_CLAIMS_MART\FLF_CLAIM_ACTIVITY.sql:
  47: {{ dbt_utils.generate_surrogate_key ( [ 'USER_LGN_NM' ] ) }} 
  50: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'ACTION_TYPE','ACTIVITY_NAME_TYPE','ACTIVITY_CONTEXT_TYPE_NAME','ACTIVITY_SUBCONTEXT_TYPE_NAME','PROCESS_AREA','USER_FUNCTIONAL_ROLE_DESC' ] ) }} 
  54: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'ACTIVITY_DETAIL_DESC' ] ) }} 
  71: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CURRENT_CORESUITE_CLAIM_TYPE_CODE','CLAIM_TYPE_CHNG_OVR_IND','CLAIM_STATE_CODE','CLAIM_STATUS_CODE','CLAIM_STATUS_REASON_CODE' ] ) }} 
  73: {{ dbt_utils.generate_surrogate_key ( [ 'FILING_SOURCE_DESC','FILING_MEDIA_DESC','NATURE_OF_INJURY_CATEGORY','NATURE_OF_INJURY_TYPE','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_IND','SB223_IND','EMPLOYER_PREMISES_IND','CATASTROPHIC_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC' ] ) }} 
  76: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ ' ORG_UNT_NM' ] ) }}      
  79: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'POLICY_TYPE_CODE','PLCY_STS_TYP_CD','PLCY_STS_RSN_TYP_CD','POLICY_ACTIVE_IND' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_CLAIMS_MART\FLF_CLAIM_ICD_HISTORY.sql:
   66: ELSE {{dbt_utils.generate_surrogate_key ( ['CLM_ICD_DESC'])}}                                  
   89: {{ dbt_utils.generate_surrogate_key ( [ 'ICD_STS_TYP_CD','ICD_LOC_TYP_CD','ICD_SITE_TYP_CD','CLM_ICD_STS_PRI_IND','CURRENT_ICD_IND' ] ) }} 
  103: {{dbt_utils.generate_surrogate_key (['CURRENT_CLAIM_TYPE_CODE', 'CURRENT_CHANGE_OVER_IND', 'CURRENT_CLAIM_STATE_TYPE_CODE', 'CURRENT_CLAIM_STATUS_TYPE_CODE', 'CURRENT_CLAIM_REASON_TYPE_CODE'])}} 
  107: {{dbt_utils.generate_surrogate_key (['CLM_TYP_CD', 'CHNG_OVR_IND', 'CLM_STT_TYP_CD', 'CLM_STS_TYP_CD', 'CLM_TRANS_RSN_TYP_CD'])}} 
  111: {{dbt_utils.generate_surrogate_key (['OCCR_SRC_TYP_NM', 'OCCR_MEDA_TYP_NM', 'NOI_CTG_TYP_NM', 'NOI_TYP_NM', 'FIREFIGHTER_CANCER_IND', 'COVID_EXPOSURE_IND', 'COVID_EMERGENCY_WORKER_IND', 'COVID_HEALTH_CARE_WORKER_IND', 'COMBINED_CLAIM_IND', 'SB223_IND', 'EMPLOYER_PREMISES_IND', 'CLM_CTRPH_INJR_IND', 'K_PROGRAM_ENROLLMENT_DESC', 'K_PROGRAM_TYPE_DESC', 'K_PROGRAM_REASON_DESC'])}} 
  115: {{dbt_utils.generate_surrogate_key (['POLICY_TYPE_CODE', 'PLCY_STS_TYP_CD', 'PLCY_STS_RSN_TYP_CD', 'POLICY_ACTIVE_IND'])}} 
  119: {{dbt_utils.generate_surrogate_key (['INDUSTRY_GROUP_CODE'])}} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_CLAIMS_MART\FLF_CLAIM_PARTICIPATION.sql:
  30: {{ dbt_utils.generate_surrogate_key ( [ 'PARTICIPATION_TYPE_CODE','PARTICIPATION_PRIMARY_IND'] ) }}                                     
  37: {{dbt_utils.generate_surrogate_key ( ['CREATE_USER_LOGIN_NAME'])}}                                  
  40: {{dbt_utils.generate_surrogate_key ( ['UPDATE_USER_LOGIN_NAME'])}}                                  

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_CLAIMS_MART\FLF_CLAIM_POLICY_ASSIGNMENT.sql:
  46: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CRNT_PLCY_IND','CTL_ELEM_SUB_TYP_CD' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FACT_CONSOLIDATED_MEDICAL_BILLING.sql:
  164: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                             MODIFIER_SEQUENCE_HKEY		
  167: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                            REFERRING_PROVIDER_HKEY
  175: {{dbt_utils.generate_surrogate_key ( ['-2222'])}}           as               FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
  220: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                            REFERRING_PROVIDER_HKEY		
  221: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                                         ICD_1_HKEY
  222: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                                         ICD_2_HKEY
  223: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                                         ICD_3_HKEY
  224: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                                         ICD_4_HKEY	
  275: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                                         ICD_1_HKEY
  276: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                                         ICD_2_HKEY
  277: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                                         ICD_3_HKEY
  278: {{dbt_utils.generate_surrogate_key ( ['99999'])}}           as                                         ICD_4_HKEY
  651: {{dbt_utils.generate_surrogate_key ( ['-2222'])}}           AS                                  FNCL_JOIN_NTWK_COL

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FACT_DETAIL_PAYMENT_CODING.sql:
  26  {{ dbt_utils.generate_surrogate_key ( [ 'ACNTB_CODE','PYMNT_FUND_TYPE','CVRG_TYPE','BILL_TYPE_F2','BILL_TYPE_L3','ACDNT_TYPE','STS_CODE' ])}} AS   PAYMENT_CODER_HKEY 
  27: {{ dbt_utils.generate_surrogate_key ( ['PAYEE_FULL_NAME'])}}AS                                         PAYEE_HKEY 
  29: {{ dbt_utils.generate_surrogate_key ( ['CLAIM_PAYMENT_CATEGORY_DESC'])}} 
  87: coalesce(PAYEE_HKEY,{{ dbt_utils.generate_surrogate_key ( [ '99999' ])}}) AS PAYEE_HKEY
  89: coalesce(CLAIM_PAYMENT_CATEGORY_HKEY,{{dbt_utils.generate_surrogate_key ( ['99999'])}}) AS CLAIM_PAYMENT_CATEGORY_HKEY
  90: coalesce(POLICY_NUMBER,{{dbt_utils.generate_surrogate_key ( ['99999'])}}) AS POLICY_NUMBER

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FACT_HOSPITAL_INPATIENT_BILLING.sql:
  121: {{dbt_utils.generate_surrogate_key ( ['ADMISSION_TYPE', 'ADMISSION_SOURCE', 'DISCHARGE_STATUS', 'BILL_TYPE'])}} 
  137: {{dbt_utils.generate_surrogate_key ( ['INVOICE_TYPE','SUBROGATION_FLAG','ADJUSTMENT_TYPE','INPUT_METHOD_CODE','PAYMENT_CATEGORY','FEE_SCHEDULE','PAID_ABOVE_ZERO_IND','SUBROGATION_TYPE_DESC'])}}  
  153: {{dbt_utils.generate_surrogate_key ( ['MOD1_MODIFIER_CODE','MOD2_MODIFIER_CODE','MOD3_MODIFIER_CODE','MOD4_MODIFIER_CODE','MOD_SET'])}} 
  180: CASE WHEN LINE_STATUS_CODE IS NULL THEN {{dbt_utils.generate_surrogate_key ( ['99999'])}}
  181:      ELSE {{dbt_utils.generate_surrogate_key ( ['LINE_STATUS_CODE'])}} END 
  184: CASE WHEN INVOICE_STATUS IS NULL THEN {{dbt_utils.generate_surrogate_key ( ['99999'])}}
  185:      ELSE {{dbt_utils.generate_surrogate_key ( ['INVOICE_STATUS'])}} END 
  204  {{dbt_utils.generate_surrogate_key ( ['CLM_TYP_CD','CHNG_OVR_IND','CLAIM_STATE_CODE','CLAIM_STATUS_CODE','CLAIM_STATUS_REASON_CODE'])}}
  208: {{dbt_utils.generate_surrogate_key ( ['POLICY_TYPE_CODE','PLCY_STS_TYP_CD','PLCY_STS_RSN_TYP_CD','POLICY_ACTIVE_IND'])}}
  292: {{dbt_utils.generate_surrogate_key ( ['OCCR_SRC_TYP_NM', 'OCCR_MEDA_TYP_NM', 'NOI_CTG_TYP_NM', 'NOI_TYP_NM','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_CLAIM_IND','SB223_IND','EMPLOYER_PREMISES_IND','CLM_CTRPH_INJR_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC'])}}
  332: CASE WHEN PROVIDER_HKEY IS NULL THEN {{dbt_utils.generate_surrogate_key ( ['99999'])}}
  333:      ELSE {{dbt_utils.generate_surrogate_key ( ['PROVIDER_HKEY'])}} END
  620  COALESCE(PROVIDER_SUBMITTED_DRG_HKEY, {{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS PROVIDER_SUBMITTED_DRG_HKEY
  621: COALESCE(BWC_CALC_DRG_OUTPUT_HKEY,{{dbt_utils.generate_surrogate_key ( ['99999'])}}) AS BWC_CALC_DRG_OUTPUT_HKEY 
  622: COALESCE(REVENUE_CENTER_HKEY, {{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS REVENUE_CENTER_HKEY
  624: COALESCE(SUBMITTING_NETWORK_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS SUBMITTING_NETWORK_HKEY
  627: COALESCE(SERVICE_PROVIDER_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS SERVICE_PROVIDER_HKEY
  628: COALESCE(PAY_TO_PROVIDER_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS PAY_TO_PROVIDER_HKEY
  629: COALESCE(CPT_SERVICE_RENDERED_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS  CPT_SERVICE_RENDERED_HKEY
  635: COALESCE(REFERRING_PROVIDER_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS REFERRING_PROVIDER_HKEY
  636: COALESCE(ADMITTING_ICD_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS ADMITTING_ICD_HKEY
  637: COALESCE(PRINCIPAL_ICD_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS PRINCIPAL_ICD_HKEY
  640: COALESCE(PAY_TO_NETWORK_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS PAY_TO_NETWORK_HKEY
  644: COALESCE(INJURED_WORKER_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS INJURED_WORKER_HKEY
  645: COALESCE(CLAIM_DETAIL_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS CLAIM_DETAIL_HKEY
  646: COALESCE(CLAIM_TYPE_STATUS_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS CLAIM_TYPE_STATUS_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FACT_HOSPITAL_OUTPATIENT_BILLING.sql:
  120: {{dbt_utils.generate_surrogate_key ( ['ADMISSION_TYPE', 'ADMISSION_SOURCE', 'DISCHARGE_STATUS', 'BILL_TYPE'])}} 
  135: {{dbt_utils.generate_surrogate_key ( ['APC_STATUS','NON_OPPS_BILL_TYPE','OPPS_FLAG', 'PAYMENT_METHOD','DISCOUNT', 'COMPOSITE_ADJUSTMENT', 'PAYMENT_INDICATOR', 'PACKAGING', 'PAYMENT_ADJUSTMENT'])}}  END
  139: {{dbt_utils.generate_surrogate_key ( ['INVOICE_TYPE','SUBROGATION_FLAG','ADJUSTMENT_TYPE','INPUT_METHOD_CODE','PAYMENT_CATEGORY','FEE_SCHEDULE','PAID_ABOVE_ZERO_IND','SUBROGATION_TYPE_DESC'])}}  
  154: {{dbt_utils.generate_surrogate_key ( ['MOD1_MODIFIER_CODE','MOD2_MODIFIER_CODE','MOD3_MODIFIER_CODE','MOD4_MODIFIER_CODE','MOD_SET'])}} 
  181: CASE WHEN LINE_STATUS_CODE IS NULL THEN {{dbt_utils.generate_surrogate_key ( ['99999'])}}
  182:      ELSE {{dbt_utils.generate_surrogate_key ( ['LINE_STATUS_CODE'])}} END AS LINE_STATUS_CODE_HKEY 
  184  CASE WHEN INVOICE_STATUS IS NULL THEN {{dbt_utils.generate_surrogate_key ( ['99999'])}}
  185:      ELSE {{dbt_utils.generate_surrogate_key ( ['INVOICE_STATUS'])}} END AS INVOICE_STATUS_HKEY
  203: {{dbt_utils.generate_surrogate_key ( ['OCCR_SRC_TYP_NM','OCCR_MEDA_TYP_NM','NOI_CTG_TYP_NM','NOI_TYP_NM','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_CLAIM_IND','SB223_IND','EMPLOYER_PREMISES_IND','CLM_CTRPH_INJR_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC'])}} 
  207: {{dbt_utils.generate_surrogate_key ( ['CLM_TYP_CD','CHNG_OVR_IND','CLAIM_STATE_CODE','CLAIM_STATUS_CODE','CLAIM_STATUS_REASON_CODE'])}}
  211: {{dbt_utils.generate_surrogate_key ( ['POLICY_TYPE_CODE','PLCY_STS_TYP_CD','PLCY_STS_RSN_TYP_CD','POLICY_ACTIVE_IND'])}} 
  595: COALESCE(REVENUE_CENTER_HKEY, {{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS REVENUE_CENTER_HKEY,
  596: COALESCE(APC_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS APC_HKEY,
  599: COALESCE(SUBMITTING_NETWORK_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS SUBMITTING_NETWORK_HKEY,
  602: COALESCE(SERVICE_PROVIDER_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS SERVICE_PROVIDER_HKEY,
  603: COALESCE(PAY_TO_PROVIDER_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS PAY_TO_PROVIDER_HKEY,
  604: COALESCE(CPT_SERVICE_RENDERED_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS CPT_SERVICE_RENDERED_HKEY,
  610: COALESCE(ADMITTING_ICD_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS ADMITTING_ICD_HKEY,
  611: COALESCE(PRINCIPAL_ICD_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS PRINCIPAL_ICD_HKEY,
  614: COALESCE(PAY_TO_NETWORK_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS PAY_TO_NETWORK_HKEY,
  618: COALESCE(INJURED_WORKER_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS INJURED_WORKER_HKEY,
  620: COALESCE(CLAIM_TYPE_STATUS_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS CLAIM_TYPE_STATUS_HKEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FACT_MEDICAL_INVOICE_MONTHLY_SUMMARY.sql:
   65: ELSE {{dbt_utils.generate_surrogate_key ( ['CLAIM_TYPE_CODE'])}}                                  
  123: coalesce(CLAIM_TYPE_HKEY,{{dbt_utils.generate_surrogate_key ( ['99999'])}}) AS CLAIM_TYPE_HKEY
  138: coalesce(CLAIM_TYPE_HKEY,{{dbt_utils.generate_surrogate_key ( ['99999'])}})

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FACT_NDC_PRICING.sql:
   39: {{dbt_utils.generate_surrogate_key ( ['PRICE_TYPE_CODE'])}} as                                  PRICING_TYPE_HKEY 
  112: WHEN NDC_DATERANGE_NDC_GPI_HKEY IS NOT NULL THEN {{dbt_utils.generate_surrogate_key ( ['-2222'])}}
  113: ELSE {{dbt_utils.generate_surrogate_key ( ['-1111'])}} END)::VARCHAR(32) AS NDC_GPI_HKEY
  115: coalesce(PRICING_TYPE_HKEY,{{dbt_utils.generate_surrogate_key ( ['-1111'])}}) AS PRICING_TYPE_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FACT_PRESCRIPTION_BILLING.sql:
  136: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'SERVICE_LEVEL_CODE','PHARMACIST_SERVICE_CODE','SERVICE_REASON_CODE','SERVICE_RESULT_CODE','SUBMITTED_DAW_CODE','SUBMISSION_CLARIFICATION_CODE','PBM_BENEFIT_PLAN_TYPE_DESC','PBM_ORIGINATION_TYPE_DESC','PHARM_SPECIAL_PROGRAM_DESC','PBM_LOCK_IN_IND','PRESCRIPTION_REFILL_NUMBER' ] ) }} 
  160: ELSE {{ dbt_utils.generate_surrogate_key ( [ 'PRICE_TYPE','CLIENT_PRICING' ] ) }} 
  165: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'INVOICE_TYPE','SUBROGATION_FLAG','ADJUSTMENT_TYPE','INPUT_METHOD_CODE','PAYMENT_CATEGORY','FEE_SCHEDULE','PAID_ABOVE_ZERO_IND','SUBROGATION_TYPE_DESC' ] ) }} 
  195: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'LINE_STATUS_CODE' ] ) }} 
  199: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'INVOICE_STATUS' ] ) }} 
  227: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CLM_TYP_CD','CHNG_OVR_IND', 'CLM_STT_TYP_CD','CLM_STS_TYP_CD','CLM_TRANS_RSN_TYP_CD' ] ) }} 
  232: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'OCCR_SRC_TYP_NM','OCCR_MEDA_TYP_NM','NOI_CTG_TYP_NM','NOI_TYP_NM','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_CLAIM_IND','SB223_IND','EMPLOYER_PREMISES_IND','CLM_CTRPH_INJR_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC' ] ) }} 
  236: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'POLICY_TYPE_CODE','PLCY_STS_TYP_CD','PLCY_STS_RSN_TYP_CD','POLICY_ACTIVE_IND' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FACT_PROFESSIONAL_ASC_BILLING.sql:
  129: {{ dbt_utils.generate_surrogate_key ( [ 'INVOICE_TYPE','SUBROGATION_FLAG','ADJUSTMENT_TYPE','INPUT_METHOD_CODE','PAYMENT_CATEGORY','FEE_SCHEDULE','PAID_ABOVE_ZERO_IND'
  171: {{ dbt_utils.generate_surrogate_key ( [ 'LINE_STATUS_CODE' ] ) }} END
  175: {{ dbt_utils.generate_surrogate_key ( [ 'INVOICE_STATUS' ] ) }} END 
  198: {{ dbt_utils.generate_surrogate_key ( ['CLM_TYP_CD','CHNG_OVR_IND','CLM_STT_TYP_CD','CLM_STS_TYP_CD','CLM_TRANS_RSN_TYP_CD' ] ) }}                                     
  202: {{ dbt_utils.generate_surrogate_key ( [ 'OCCR_SRC_TYP_NM','OCCR_MEDA_TYP_NM','NOI_CTG_TYP_NM','NOI_TYP_NM','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_CLAIM_IND','SB223_IND','EMPLOYER_PREMISES_IND','CLM_CTRPH_INJR_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC' ] ) }} 
  207: {{ dbt_utils.generate_surrogate_key ( [ 'POLICY_TYPE_CODE','PLCY_STS_TYP_CD','PLCY_STS_RSN_TYP_CD','POLICY_ACTIVE_IND' ] ) }}   
  626: {{ dbt_utils.generate_surrogate_key ( [ 'INVOICE_MOD1_MODIFIER_CODE','INVOICE_MOD2_MODIFIER_CODE','INVOICE_MOD3_MODIFIER_CODE','INVOICE_MOD4_MODIFIER_CODE'

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FLF_HEALTHCARE_SERVICE_AUTHORIZATION.sql:
   67: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'AUTHORIZATION_STATUS_CODE','AUTHORIZATION_SERVICE_TYPE_CODE','VOID_IND' ] ) }} 
   86: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'APP_CNTX_TYP_CD','CASE_CATEGORY_CODE','CASE_TYPE_CODE','CASE_PRTY_TYP_CD','CASE_RSOL_TYP_CD' ] ) }} 
   89: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CASE_STT_TYP_CD','CASE_STS_TYP_CD','CASE_STS_RSN_TYP_CD' ] ) }} 
   92: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CLM_TYP_CD','CHNG_OVR_IND','CLM_STT_TYP_CD','CLM_STS_TYP_CD','CLM_TRANS_RSN_TYP_CD' ] ) }} 
  115: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'POLICY_TYPE_CODE','PLCY_STS_TYP_CD','PLCY_STS_RSN_TYP_CD','POLICY_ACTIVE_IND' ] ) }} 
  120: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'OCCR_SRC_TYP_NM','OCCR_MEDA_TYP_NM','NOI_CTG_TYP_NM','NOI_TYP_NM','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_CLAIM_IND', 'SB223_IND', 'EMPLOYER_PREMISES_IND','CLM_CTRPH_INJR_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC' ] ) }} 
  123: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'OCCR_PRMS_TYP_NM','CLM_OCCR_LOC_CNTRY_NM','CLM_OCCR_LOC_STT_CD','CLM_OCCR_LOC_STT_NM','CLM_OCCR_LOC_CNTY_NM','CLM_OCCR_LOC_CITY_NM','CLM_OCCR_LOC_POST_CD','CLM_OCCR_LOC_NM','CLM_OCCR_LOC_STR_1','CLM_OCCR_LOC_STR_2','CLM_OCCR_LOC_COMT' ] ) }} 
  292: coalesce(HEALTHCARE_AUTHORIZATION_STATUS_HKEY,{{ dbt_utils.generate_surrogate_key ( [ '99999' ])}}) AS HEALTHCARE_AUTHORIZATION_STATUS_HKEY
  296: coalesce(CASE_TYPE_HKEY,{{ dbt_utils.generate_surrogate_key ( [ '99999' ])}}) AS CASE_TYPE_HKEY
  297: coalesce(CASE_STATUS_HKEY,{{ dbt_utils.generate_surrogate_key ( [ '99999' ])}}) AS CASE_STATUS_HKEY
  303: WHEN FILTER_EMP_EMP_CUSTOMER_NUMBER IS NOT NULL THEN {{dbt_utils.generate_surrogate_key ( ['-2222'])}} 
  304: else {{dbt_utils.generate_surrogate_key ( ['99999'])}}
  306: coalesce(POLICY_STANDING_HKEY,{{ dbt_utils.generate_surrogate_key ( [ '99999' ])}}) AS POLICY_STANDING_HKEY
  308: WHEN FILTER_IW_CORESUITE_CUSTOMER_NUMBER IS NOT NULL THEN {{dbt_utils.generate_surrogate_key ( ['-2222'])}} 
  309: else {{dbt_utils.generate_surrogate_key ( ['99999'])}}
  312: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CLM_LOSS_DESC', 'CLM_CLMT_JOB_TTL' ] ) }} 
  314: coalesce(CLAIM_TYPE_STATUS_HKEY,{{ dbt_utils.generate_surrogate_key ( [ '99999' ])}}) AS CLAIM_TYPE_STATUS_HKEY
  316: coalesce(ACCIDENT_LOCATION_HKEY,{{ dbt_utils.generate_surrogate_key ( [ '99999' ])}}) AS ACCIDENT_LOCATION_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FLF_MEDICAL_INVOICE_HEADER_EDIT_EOB.sql:
  61: {{dbt_utils.generate_surrogate_key ( ['HDR_EDIT_SOURCE', 'HDR_EDIT_PHASE_APPLIED', 'HDR_EDIT_DISPOSITION'])}} 
  63: {{dbt_utils.generate_surrogate_key ( ['HDR_EOB_SOURCE', 'HDR_EOB_PHASE_APPLIED', 'EOB_DISPOSITION'])}} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FLF_MEDICAL_INVOICE_LINE_EDIT_EOB.sql:
  142: WHEN FILTER_EDIT_EDIT_CODE IS NOT NULL THEN {{dbt_utils.generate_surrogate_key ( ['-2222'])}} 
  143: else {{dbt_utils.generate_surrogate_key ( ['88888'])}}
  146: WHEN FILTER_EOB_EOB_CODE IS NOT NULL THEN {{dbt_utils.generate_surrogate_key ( ['-2222'])}} 
  147: else {{dbt_utils.generate_surrogate_key ( ['88888'])}}
  149: {{dbt_utils.generate_surrogate_key ( ['LINE_EDIT_SOURCE','LINE_EDIT_PHASE_APPLIED', 'LINE_EDIT_DISPOSITION'])}} AS EDIT_ENTRY_HKEY, 
  150: {{dbt_utils.generate_surrogate_key ( ['LINE_EOB_SOURCE','LINE_EOB_PHASE_APPLIED', 'EOB_DISPOSITION'])}} AS EOB_ENTRY_HKEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FLF_MEDICAL_INVOICE_PROCEDURES.sql:
  113:  when FILTER_PRO_HEADER_PROCEDURE_HKEY is null then {{dbt_utils.generate_surrogate_key ( ['-2222'])}}
  114:  else {{dbt_utils.generate_surrogate_key ( ['-1111'])}} end as  HEADER_PROCEDURE_HKEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FLF_PROVIDER_CERTIFICATION_LOG.sql:
  103: coalesce( PROVIDER_HKEY, {{dbt_utils.generate_surrogate_key ( ['99999'])}} ) AS PROVIDER_HKEY
  104: coalesce( BWC_CERTIFICATION_STATUS_HKEY, {{dbt_utils.generate_surrogate_key ( ['99999'])}} ) AS BWC_CERTIFICATION_STATUS_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_MEDICAL_MART\FLF_PROVIDER_ENROLLMENT_LOG.sql:
  102:  coalesce( PROVIDER_HKEY, {{dbt_utils.generate_surrogate_key ( ['99999'])}} ) AS PROVIDER_HKEY
  103: ,coalesce( ENROLLMENT_STATUS_HKEY, {{dbt_utils.generate_surrogate_key ( ['99999'])}} ) AS ENROLLMENT_STATUS_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_POLICY_MART\FACT_EARNED_PREMIUM.sql:
  187: {{ dbt_utils.generate_surrogate_key ( ['CUSTOMER_NUMBER','RECORD_EFFECTIVE_DATE'])}} as EMPLOYER_HKEY
  189: 	then MD5( '99998' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'POLICY_PERIOD_EFFECTIVE_DATE','POLICY_PERIOD_END_DATE','PEC_POLICY_IND','NEW_POLICY_IND','POLICY_PERIOD_DESC' ] ) }} 
  191: coalesce({{ dbt_utils.generate_surrogate_key ( ['POLICY_TYPE_CODE', 'POLICY_STATUS_CODE','STATUS_REASON_CODE', 'POLICY_ACTIVE_IND'])}},{{dbt_utils.generate_surrogate_key ( ['99999'])}}) as POLICY_STANDING_HKEY
  192: {{ dbt_utils.generate_surrogate_key ( ['PAYMENT_PLAN_TYPE_DESC', 'REPORTING_FREQUENCY_TYPE_DESC', 'AUDIT_TYPE_DESC', 'EMPLOYEE_LEASING_TYPE_DESC', 'POLICY_EMPLOYER_PAID_PROGRAM_IND', 'ESTIMATED_ZERO_PAYROLL_IND', 'REPORTED_ZERO_PAYROLL_IND', 'ESTIMATED_PREMIUM_IND'])}} 
  194: {{ dbt_utils.generate_surrogate_key ( ['FINANCIAL_TRANSACTION_TYPE_ID'])}} as FINANCIAL_TRANSACTION_TYPE_HKEY
  195: CASE WHEN  FINANCIAL_TRANSACTION_STATUS_CODE IS NULL THEN MD5('88888') ELSE {{ dbt_utils.generate_surrogate_key ( ['FINANCIAL_TRANSACTION_STATUS_CODE'])}} END as FINANCIAL_TRANSACTION_STATUS_HKEY
  200: CASE WHEN PFT_COMMENT_TEXT IS NULL THEN MD5('N/A') ELSE {{ dbt_utils.generate_surrogate_key ( ['PFT_COMMENT_TEXT'])}} END AS  FINANCIAL_TRANSACTION_COMMENT_HKEY
  201: coalesce(FINANCIAL_TRANSACTION_CREATE_USER_HKEY,{{dbt_utils.generate_surrogate_key ( ['99999'])}}) AS FINANCIAL_TRANSACTION_CREATE_USER_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_POLICY_MART\FACT_POLICY_CORRESPONDENCE.sql:
   49: {{dbt_utils.generate_surrogate_key ( ['DOCM_TYP_REF_NO', 'DOCM_TYP_VER_NO'])}}
   58: {{dbt_utils.generate_surrogate_key ( ['DOCM_STT_TYP_CD', 'DOCM_STS_TYP_CD','DOCM_SYS_GEN_IND','DOCM_TYP_VER_DUP_IND','DOCM_TYP_VER_SYS_GEN_IND','DOCM_TYP_VER_SYS_GEN_ONLY_IND','DOCM_TYP_VER_CANC_PLCY_IND','DOCM_TYP_VER_PRE_PRNT_ENCL_IND','DOCM_TYP_VER_BTCH_PRNT_IND','DOCM_TYP_VER_USR_CAN_DEL_IND','DOCM_TYP_VER_MULTI_RDR_VER_IND','DOCM_TYP_VER_PRNT_DUPLX_IND'])}}
   62: {{dbt_utils.generate_surrogate_key ( ['PTCP_TYP_CD','PLCY_PRD_PTCP_INS_PRI_IND'])}} AS     PARTICIPATION_TYPE_HKEY
   97: {{dbt_utils.generate_surrogate_key ( ['CR_USER_LGN_NM'])}}
  109: {{dbt_utils.generate_surrogate_key ( ['UP_USER_LGN_NM'])}}

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_POLICY_MART\FACT_POLICY_PERIOD_RATING_ELEMENTS.sql:
  35: {{ dbt_utils.generate_surrogate_key ( [ 'RT_ELEM_TYP_CD' ] ) }} 
  48: {{ dbt_utils.generate_surrogate_key ( [ 'PLCY_PRD_EFF_DT', 'PLCY_PRD_END_DT', 'PEC_POLICY_IND', 'NEW_POLICY_IND', 'POLICY_PERIOD_DESC' ] ) }} 
  50: {{ dbt_utils.generate_surrogate_key ( [ 'POLICY_TYPE_CODE','PAYMENT_PLAN_CODE','LEASE_TYPE_CODE' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_POLICY_MART\FACT_POLICY_PERIOD_STATUS.sql:
   47: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'PLCY_TYP_CODE','PLCY_STS_TYP_CD','PLCY_STS_RSN_TYP_CD','POLICY_ACTIVE_IND' ] ) }} 
   51: Then MD5( '99999' )  Else  {{ dbt_utils.generate_surrogate_key ( [ 'CR_USER_LGN_NM' ] ) }} 
   54: Then MD5( '99999' )  Else  {{ dbt_utils.generate_surrogate_key ( [ 'UP_USER_LGN_NM' ] ) }} 
   73: then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'PLCY_PRD_EFF_DATE','PLCY_PRD_END_DATE','PEC_POLICY_IND','NEW_POLICY_IND', 'POLICY_PERIOD_DESC' ] ) }} 
  151: COALESCE(EMPLOYER_HKEY, {{dbt_utils.generate_surrogate_key ( ['-1111'])}} ) AS EMPLOYER_HKEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\EDW_STG_POLICY_MART\FACT_WRITTEN_PREMIUM.sql:
   51: {{ dbt_utils.generate_surrogate_key ( [ 'POLICY_PERIOD_EFFECTIVE_DATE','POLICY_PERIOD_END_DATE','PEC_POLICY_IND','NEW_POLICY_IND', 'POLICY_PERIOD_DESC' ] ) }}                                     
   67: THEN {{ dbt_utils.generate_surrogate_key ( [ 'COVERED_INDIVIDUAL_CUSTOMER_NUMBER' ] ) }}
   84: {{ dbt_utils.generate_surrogate_key ( [ 'WRITTEN_PREMIUM_ELEMENT_CODE','WRITTEN_PREMIUM_ELEMENT_SUFFIX_CODE','WRITTEN_PREMIUM_ELEMENT_EFFECTIVE_DATE' ] ) }}                                     
   86: CASE WHEN POLICY_TYPE_CODE IS NOT NULL and POLICY_STATUS_CODE <> 'N/A' THEN  {{ dbt_utils.generate_surrogate_key ( [ 'POLICY_TYPE_CODE','POLICY_STATUS_CODE','STATUS_REASON_CODE', 'POLICY_ACTIVE_IND' ] ) }}
   96: {{ dbt_utils.generate_surrogate_key ( [ 'INDIVIDUAL_COVERAGE_TYPE_DESC','INDIVIDUAL_COVERAGE_TITLE','INDIVIDUAL_COVERAGE_INCLUSION_IND' ] ) }}       
  100: {{ dbt_utils.generate_surrogate_key ( [ 'RATING_PLAN_CODE' ] ) }}       
  104: {{ dbt_utils.generate_surrogate_key ( [ 'CREATE_USER_LOGIN_NAME' ] ) }}       
  108: {{ dbt_utils.generate_surrogate_key ( [ 'INDUSTRY_GROUP_CODE' ] ) }}    
  124: {{ dbt_utils.generate_surrogate_key ( [ 'PAYMENT_PLAN_TYPE_DESC','REPORTING_FREQUENCY_TYPE_DESC','AUDIT_TYPE_DESC', 'EMPLOYEE_LEASING_TYPE_DESC', 'POLICY_EMPLOYER_PAID_PROGRAM_IND' ] ) }} 
  225: ELSE	 {{ dbt_utils.generate_surrogate_key ( [ 'PREMIUM_CALCULATION_TYPE_DESC','CURRENT_PREMIUM_CALCULATION_IND','EXPOSURE_TYPE_DESC','EXPOSURE_AUDIT_TYPE_DESC' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\ODS_ACTUARIAL\BWC_STG_MIRA_RESERVE_EXTRACT.sql:
  377: {{dbt_utils.generate_surrogate_key ( ['CLAIM_NMBR', 'EXTRACT_DATE'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_ACTIVITY.sql:
  68: {{ dbt_utils.generate_surrogate_key ( ['ACTV_ACTN_TYP_NM','ACTV_NM_TYP_NM','CNTX_TYP_NM','ACTV_DTL_COL_NM','SUBLOC_TYP_NM','ACTV_DTL_DESC','FNCT_ROLE_NM'] ) }} As UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_ADMISSION.sql:
  22: {{dbt_utils.generate_surrogate_key ( ['ADMISSION_TYPE','ADMISSION_SOURCE','DISCHARGE_STATUS', 'BILL_TYPE'])}}   as  UNIQUE_ID_KEY ,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_AMBULATORY_PAYMENT_CLASSIFICATION.sql:
  36: {{dbt_utils.generate_surrogate_key ( ['APC_CODE'])}} as UNIQUE_ID_KEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_BENEFIT_TYPE.sql:
  42: {{ dbt_utils.generate_surrogate_key ( ['BNFT_TYP_CD','JUR_TYP_CD','BNFT_RPT_TYP_NM'] ) }} As UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_BWC_CALC_DRG_OUTPUT.sql:
  24: {{dbt_utils.generate_surrogate_key ( ['INVOICE_NUMBER'])}} AS UNIQUE_ID_KEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CASE_EXAM_SCHEDULE.sql:
  54: {{ dbt_utils.generate_surrogate_key ( [ 'CD_EXM_REQS_TYP_CD','CPS_TYP_CD','CPS_TYP_CD_SCND','CD_ADNDM_REQS_TYP_CD' ] ) }} 
  56: {{ dbt_utils.generate_surrogate_key ( [ 'CDES_CLMT_AVL_MON_IND','CDES_CLMT_AVL_TUE_IND','CDES_CLMT_AVL_WED_IND','CDES_CLMT_AVL_THU_IND','CDES_CLMT_AVL_FRI_IND','CDES_CLMT_AVL_SAT_IND','CDES_CLMT_AVL_SUN_IND','CDES_ITPRT_NEED_IND','CDES_GRTT_45_IND','CDES_TRVL_REMB_IND','CDES_ADDTNL_TST_IND','CDES_ADNDM_REQS_IND','CDES_RSLT_SUSPD_IND' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CASE_STATUS.sql:
  43: {{dbt_utils.generate_surrogate_key ( ['CASE_STT_TYP_CD','CASE_STS_TYP_CD','CASE_STS_RSN_TYP_CD'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CASE_TYPE.sql:
  11: {{ dbt_utils.generate_surrogate_key ( [ 'APP_CNTX_TYP_CD','CASE_CTG_TYP_CD','CASE_TYP_CD','CASE_PRTY_TYP_CD','CASE_RSOL_TYP_CD' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_ACCIDENT_DESCRIPTION.sql:
  43: {{ dbt_utils.generate_surrogate_key ( [ 'CLM_LOSS_DESC','CLM_CLMT_JOB_TTL' ] ) }}  as UNIQUE_ID_KEY  

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_ACCIDENT_LOCATION.sql:
  11: {{ dbt_utils.generate_surrogate_key ( [ 'OCCR_PRMS_TYP_NM','CLM_OCCR_LOC_CNTRY_NM','CLM_OCCR_LOC_STT_CD','CLM_OCCR_LOC_STT_NM','CLM_OCCR_LOC_CNTY_NM','CLM_OCCR_LOC_CITY_NM','CLM_OCCR_LOC_POST_CD','CLM_OCCR_LOC_NM','CLM_OCCR_LOC_STR_1','CLM_OCCR_LOC_STR_2','CLM_OCCR_LOC_COMT' ] ) }}  

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_ACTIVITY.sql:
  558: {{dbt_utils.generate_surrogate_key ( [ 'ACTV_ID','ACTV_DTL_ID','CLM_NO' ])}} AS  UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_ADDITIONAL_ALLOWANCE_REFERRAL.sql:
  366: {{dbt_utils.generate_surrogate_key ( ['CLAIM_NUMBER','DOCM_NO', 'FORM', 'DATE_FILED' ])}}  as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_ADDITIONAL_ALLOWANCE.sql:
  529: {{ dbt_utils.generate_surrogate_key ( [ 'CLAIM_NUMBER','DOCM_NO','FORM','DATE_FILED','ICD_CODE','CLM_ICD_DESC','ICD_LOC_TYP_CD','ICD_SITE_TYP_CD' ] ) }} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_DETAIL_PAYMENT_CODING.sql:
  254: {{dbt_utils.generate_surrogate_key ( ['CLAIM_NO', 'TCN_NO', 'CHECK_NO', 'WRNT_DATE', 'ACNTB_CODE', 'BILL_TYPE_F2', 'BILL_TYPE_L3', 'ACDNT_TYPE', 'PYMNT_CODE_AMT' ])}}  as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_ICD_STATUS_DETAIL.sql:
  44:  {{ dbt_utils.generate_surrogate_key ( [ 'ICD_STS_TYP_CD','ICD_LOC_TYP_CD','ICD_SITE_TYP_CD','CLM_ICD_STS_PRI_IND','CURRENT_ICD_STATUS_IND' ] ) }}  as UNIQUE_ID_KEY, * 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_ICD_STATUS_HISTORY.sql:
  221: {{ dbt_utils.generate_surrogate_key ( ['HIST_ID'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_ICD_STATUS_TYPE.sql:
  12: {{ dbt_utils.generate_surrogate_key ( [ 'ICD_STS_TYP_CD','CLM_ICD_STS_PRI_IND' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_INJURY_HISTORY.sql:
  173: {{ dbt_utils.generate_surrogate_key ( ['CLM_NO', 'CLM_ICD_STS_ID', 'ICD_CODE', 'CLM_ICD_DESC', 'ICD_LOC_TYP_IND', 'HIST_EFF_DTM', 'HIST_END_DTM','ICD_SITE_TYP_CD', 'ICD_STS_TYP_CD','ICD_CTRPH_IND'])}} as UNIQUE_ID_KEY, * FROM UNION_ODS

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_INSURED_PARTICIPATION.sql:
  187: {{ dbt_utils.generate_surrogate_key ( ['CLM_AGRE_ID','CP_EFF_DT'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_INVESTIGATION.sql:
  133: {{ dbt_utils.generate_surrogate_key ( ['CLM_NO'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_POLICY_HISTORY.sql:
  294: {{ dbt_utils.generate_surrogate_key ( ['CLM_NO','CLM_PLCY_RLTNS_EFF_DT'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_STATUS.sql:
  43: {{ dbt_utils.generate_surrogate_key ( ['CLM_STT_TYP_CD','CLM_STT_TYP_NM','CLM_STS_TYP_CD','CLM_STS_TYP_NM', 'CLM_TRANS_RSN_TYP_CD', 'CLM_TRANS_RSN_TYP_NM'])}} AS CLAIM_STATUS_HKEY,
  44: {{ dbt_utils.generate_surrogate_key ( ['CLM_STT_TYP_CD','CLM_STT_TYP_NM','CLM_STS_TYP_CD','CLM_STS_TYP_NM', 'CLM_TRANS_RSN_TYP_CD', 'CLM_TRANS_RSN_TYP_NM'])}} AS UNIQUE_ID_KEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_TYPE_STATUS.sql:
  222: {{ dbt_utils.generate_surrogate_key ( ['CLM_TYP_CD','CLAIM_TYPE_CHANGE_OVER_IND','CLM_STT_TYP_CD','CLM_STS_TYP_CD','CLM_TRANS_RSN_TYP_CD'] ) }} As UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM_TYPE.sql:
  12: {{ dbt_utils.generate_surrogate_key ( [ 'CLM_TYP_CD' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CLAIM.sql:
  1253: {{ dbt_utils.generate_surrogate_key ( ['CLM_NO'])}} as UNIQUE_ID_KEY,		

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_COVERED_INDIVIDUAL_CUSTOMER.sql:
  273: {{ dbt_utils.generate_surrogate_key ( ['CUST_NO'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CPT_CODE.sql:
  105: {{ dbt_utils.generate_surrogate_key ( ['SERVICE_CODE','SERVICE_MOD'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_CUSTOMER.sql:
  31: {{ dbt_utils.generate_surrogate_key ( [ 'CUST_NO' ] ) }}   as                                      UNIQUE_ID_KEY                                     

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_DIAGNOSIS_RELATED_GROUP.sql:
  45: {{ dbt_utils.generate_surrogate_key(['DRG_CODE'])}} as UNIQUE_ID_KEY, * FROM JOIN_DRG)

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_DISABILITY_TYPE.sql:
  106: {{ dbt_utils.generate_surrogate_key ( ['CLM_DISAB_MANG_DISAB_TYP_CD', 'CLM_DISAB_MANG_RSN_TYP_CD', 'CLM_DISAB_MANG_MED_STS_TYP_CD', 'CLM_DISAB_MANG_WK_STS_TYP_CD', 'CURRENT_DISABILITY_STATUS_IND'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_DOCUMENT_DETAIL.sql:
  64: {{ dbt_utils.generate_surrogate_key ( ['DOCM_STT_TYP_CD','DOCM_STS_TYP_CD','DOCM_SYS_GEN_IND','DOCM_TYP_VER_DUP_IND','DOCM_TYP_VER_SYS_GEN_IND','DOCM_TYP_VER_SYS_GEN_ONLY_IND','DOCM_TYP_VER_CANC_PLCY_IND','DOCM_TYP_VER_PRE_PRNT_ENCL_IND','DOCM_TYP_VER_BTCH_PRNT_IND','DOCM_TYP_VER_USR_CAN_DEL_IND','DOCM_TYP_VER_MULTI_RDR_VER_IND','DOCM_TYP_VER_PRNT_DUPLX_IND'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_DOCUMENT_TYPE.sql:
  61: {{ dbt_utils.generate_surrogate_key ( ['DOCM_TYP_REF_NO','DOCM_TYP_VER_NO'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_DRUG_PRICING_TYPE.sql:
  9: {{ dbt_utils.generate_surrogate_key ( ['NDPT_CODE'])}}   as  UNIQUE_ID_KEY ,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_EARNED_PREMIUM_BILLS.sql:
  501: {{ dbt_utils.generate_surrogate_key ( ['PFT_ID'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_EARNED_PREMIUM_PAYMENTS.sql:
  188: {{ dbt_utils.generate_surrogate_key ( ['PFTA_ID', 'BILL_PFT_ID'])}} as UNIQUE_ID_KEY, * FROM ETL)

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_EDIT_EOB_ENTRY.sql:
  103: {{ dbt_utils.generate_surrogate_key ( ['SOURCE','PHASE_APPLIED','DISPOSITION'])}} as UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_EDIT.sql:
  79: {{ dbt_utils.generate_surrogate_key ( ['CODE'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_EMPLOYER.sql:
  36: {{ dbt_utils.generate_surrogate_key ( [ 'CUST_ID'])}}        as                                      UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_EOB.sql:
  133: {{ dbt_utils.generate_surrogate_key ( ['CODE'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_FINANCIAL_TRANSACTION_STATUS.sql:
  12: {{ dbt_utils.generate_surrogate_key ( [ 'FNCL_TRAN_SUB_TYP_CD' ])}} as                               UNIQUE_ID_KEY                                   

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_FINANCIAL_TRANSACTION_TYPE.sql:
  42: {{ dbt_utils.generate_surrogate_key ( [ 'FNCL_TRAN_TYP_ID' ] ) }} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_FUNCTIONAL_ROLE_ORGANIZATION_UNIT.sql:
  69: {{ dbt_utils.generate_surrogate_key ( ['FROUX_ID'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_HEALTHCARE_AUTHORIZATION_STATUS.sql:
  11: {{ dbt_utils.generate_surrogate_key ( [ 'CASE_SERV_STS_TYP_CD','CASE_SERV_TYP_CD', 'VOID_IND' ])}} AS                                      UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_HOSPITAL_ICD_PROCEDURE.sql:
  15 {{dbt_utils.generate_surrogate_key ( ['CODE'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_ICD_ADMISSION_PRESENCE.sql:
  11: {{ dbt_utils.generate_surrogate_key (['REF_IDN'])}}          AS                                      UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_ICD.sql:
  253:{{ dbt_utils.generate_surrogate_key ( ['ICD_CODE', 'ICD_CODE_VERSION_NUMBER'])}} as UNIQUE_ID_KEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT.sql:
  518: {{ dbt_utils.generate_surrogate_key ( [ 'INDM_PAY_ID', 'INDM_SCH_DTL_AMT_ID', 'AGRE_ID'])}} AS UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_INDEMNITY_PLAN_SCHEDULE_DETAIL.sql:
  138: {{dbt_utils.generate_surrogate_key ( [ 'INDM_FREQ_TYP_CD', 'INDM_RSN_TYP_CD', 'INDM_SCH_DTL_AMT_TYP_CD', 'INDM_SCH_DTL_STS_TYP_CD', 'INDM_SCH_DTL_FNL_PAY_IND', 'INDM_SCH_AUTO_PAY_IND', 'INDM_PAY_RECALC_IND','INDM_SCH_DTL_AMT_PRI_IND', 'INDM_SCH_DTL_AMT_MAILTO_IND', 'INDM_SCH_DTL_AMT_RMND_IND', 'OVR_PYMNT_IND', 'IP_VOID_IND', 'ISS_VOID_IND', 'ISD_VOID_IND', 'ISDA_VOID_IND'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_INDIVIDUAL_COVERAGE_TYPE.sql:
  47:  {{dbt_utils.generate_surrogate_key ( [ 'COV_TYP_CD','TTL_TYP_CD','PPPIE_COV_IND' ])}} AS  UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_INDUSTRY_GROUP.sql:
  42:{{dbt_utils.generate_surrogate_key ( ['SIC_TYP_CD'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_INJURED_WORKER.sql:
  362: {{dbt_utils.generate_surrogate_key ( ['CUST_ID'])}} as UNIQUE_ID_KEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_INVOICE_HOSPITAL.sql:
  606: {{dbt_utils.generate_surrogate_key ( ['INVOICE_NUMBER','LINE_SEQUENCE','SEQUENCE_EXTENSION','LINE_VERSION_NUMBER','LINE_DLM'])}} as UNIQUE_ID_KEY,
  607: {{dbt_utils.generate_surrogate_key ( ['MCO_NUMBER'])}} as MCO_ID_KEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_INVOICE_PRESCRIPTION.sql:
   48: {{ dbt_utils.generate_surrogate_key ( [ 'MCO_NUMBER' ])}}    AS                                         MCO_ID_KEY 
  653: {{ dbt_utils.generate_surrogate_key ( [ 'INVOICE_NUMBER','LINE_SEQUENCE','SEQUENCE_EXTENSION','LINE_VERSION_NUMBER','LINE_DLM' ])}} AS  UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_INVOICE_PROFILE.sql:
  69:  {{dbt_utils.generate_surrogate_key ( ['INVOICE_TYPE','SUBROGATION_FLAG','ADJUSTMENT_TYPE','INPUT_METHOD_CODE','PAYMENT_CATEGORY','FEE_SCHEDULE','PAID_ABOVE_ZERO_IND','SUBROGATION_TYPE_DESC'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_INVOICE_STATUS.sql:
  42: {{ dbt_utils.generate_surrogate_key ( ['INVOICE_STATUS_CODE'])}} as UNIQUE_ID_KEY,
  44: {{ dbt_utils.generate_surrogate_key ( ['INVOICE_STATUS_CODE'])}} as INVOICE_STATUS_HKEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_INVOICE.sql:
   38: {{ dbt_utils.generate_surrogate_key ( [ 'MCO_NUMBER' ])}}      AS                                         MCO_ID_KEY 
  486: {{ dbt_utils.generate_surrogate_key ( [ 'INVOICE_NUMBER','LINE_SEQUENCE','SEQUENCE_EXTENSION','LINE_VERSION_NUMBER','LINE_DLM' ])}} AS  UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_MANUAL_CLASSIFICATION.sql:
  13: {{ dbt_utils.generate_surrogate_key ( [ 'WC_CLS_CLS_CD','WC_CLS_SUFX_CLS_SUFX' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_MCO.txt:
  58: {{dbt_utils.generate_surrogate_key ( ['NTWK_NUMBER'])}} as UNIQUE_ID_KEY,*

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_MEDICAL_INVOICE_LINE_EDIT_EOB.sql:
  195: {{ dbt_utils.generate_surrogate_key ( [ 'INVOICE_NUMBER','LINE_SEQUENCE','EDIT_CODE','EOB_CODE','LINE_EDIT_SOURCE','LINE_EOB_SOURCE','LINE_EDIT_PHASE_APPLIED','LINE_EOB_PHASE_APPLIED','EOB_ENTRY_DATE','EDIT_ENTRY_DATE' ] ) }} as INV_INVOICE_LINE_ID

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_MISCELLANEOUS_CUSTOMER.sql:
  12: {{ dbt_utils.generate_surrogate_key ( [ 'CUST_NO' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_MODIFIER_SEQUENCE.sql:
  191: {{dbt_utils.generate_surrogate_key ( ['MOD_SET','MOD_CODE_1','MOD_CODE_2','MOD_CODE_3', 'MOD_CODE_4'])}} as UNIQUE_ID_KEY,*   FROM ETL)

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_NDC_GPI_REFERENCE.sql:
  615: {{dbt_utils.generate_surrogate_key ( ['NDC_11_CODE'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_NETWORK.sql:
  83: {{dbt_utils.generate_surrogate_key ( ['JOIN_NTWK.NTWK_NUMBER'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_ORGANIZATIONAL_UNIT.sql:
  72:  {{dbt_utils.generate_surrogate_key(['ORG_UNT_ID'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_OUTPATIENT_GROUPER.sql:
  191: {{dbt_utils.generate_surrogate_key ( ['STATUS','NON_OPPS_BILL_TYPE','OPPS_FLAG','PAYMENT_METHOD', 'DISCOUNT' , 'COMPOSITE_ADJUSTMENT','PAYMENT_INDICATOR','PACKAGING','PAYMENT_ADJUSTMENT'])}} as UNIQUE_ID_KEY, * FROM APC)

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PARTICIPATION_TYPE.sql:
  51:  {{ dbt_utils.generate_surrogate_key ( [ 'PTCP_TYP_CD','PTCP_PRI_IND'] ) }}  as  UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PAYEE_NAME.sql:
  33: DISTINCT {{dbt_utils.generate_surrogate_key(['PAYEE_NAME'])}} as UNIQUE_ID_KEY, * FROM JOIN_DPC)

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PAYMENT_CODER.sql:
  11: {{ dbt_utils.generate_surrogate_key ( [ 'ACNTB_CODE','PYMNT_FUND_TYPE','CVRG_TYPE','BILL_TYPE_F2','BILL_TYPE_L3','ACDNT_TYPE','STS_CODE' ])}} AS                                      UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PAYMENT_REQUEST_CODE.sql:
  57: {{dbt_utils.generate_surrogate_key ( ['PAY_MEDA_PREF_TYP_CD','PAY_REQS_TYP_CD','PAY_REQS_STT_TYP_CD','PAY_REQS_STS_TYP_CD','PAY_REQS_STS_RSN_TYP_CD'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PBM_PRICING_METHOD.sql:
  69: distinct {{dbt_utils.generate_surrogate_key ( ['PBM_PRCNG_CODE','PBM_PRCNG_SRC_CODE'])}} as UNIQUE_ID_KEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PLACE_OF_SERVICE.sql:
  11: {{ dbt_utils.generate_surrogate_key ( [ 'REF_IDN' ] ) }} 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_POLICY_BILLING.sql:
  325: {{ dbt_utils.generate_surrogate_key ( ['PAYMENT_PLAN_TYPE_CODE', 'REPORTING_FREQUENCY_TYPE_CODE', 'AUDIT_TYPE_CODE', 'EMPLOYEE_LEASING_TYPE_CODE', 'POLICY_15K_PROGRAM_IND', 'ESTIMATED_ZERO_PAYROLL_IND', 'REPORTED_ZERO_PAYROLL_IND', 'ESTIMATED_PREMIUM_IND'] ) }} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_POLICY_FINANCIAL_TRANSACTION_COMMENT.sql:
  33: DISTINCT {{dbt_utils.generate_surrogate_key ( ['PFT_CMT'])}} as UNIQUE_ID_KEY, 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_POLICY_PARTICIPATION_INSURED.sql:
  103: {{ dbt_utils.generate_surrogate_key ( ['CUST_ID', 'PLCY_AGRE_ID', 'PPP_EFF_DATE'] ) }} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_POLICY_PERIOD.sql:
  94: {{ dbt_utils.generate_surrogate_key ( ['PLCY_PRD_EFF_DT', 'PLCY_PRD_END_DT', 'PEC_POLICY_IND', 'NEW_POLICY_IND','POLICY_PERIOD_DESC'] ) }} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_POLICY_STANDING.sql:
  43: {{dbt_utils.generate_surrogate_key (['PLCY_TYP_CODE', 'PLCY_STS_TYP_CD', 'PLCY_STS_RSN_TYP_CD', 'ACT_IND'])}}  AS  UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_POLICY.sql:
  136: {{dbt_utils.generate_surrogate_key ( [ 'PAYMENT_PLAN_TYPE_CODE','REPORTING_FREQUENCY_TYPE_CODE','AUDIT_TYPE_CODE','EMPLOYEE_LEASING_TYPE_CODE','POLICY_EMPLOYER_PAID_PROGRAM_IND' ])}} AS  UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PREMIUM_CALCULATION_TYPE.sql:
  81:  {{ dbt_utils.generate_surrogate_key ( [ 'PREMIUM_CALCULATION_TYPE_DESC','CURRENT_PREMIUM_CALCULATION_IND','PREM_TYP_CD','PLCY_AUDT_TYP_CD'] ) }}  as  UNIQUE_ID_KEY,

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PRESCRIPTION_BILL.sql:
  123: distinct {{dbt_utils.generate_surrogate_key ( ['SERVICE_LEVEL','PROF_SERVICE','REASON_SERVICE','RESULT_SERVICE','GENERIC_DRUG_IND','CLARIFICATION','PLAN_CODE','ORIGINATION_FLAG','SPECIAL_PROGRAM',

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PROVIDER_CERTIFICATION_STATUS_LOG.sql:
  332: {{dbt_utils.generate_surrogate_key ( ['PEACH_NUMBER','DRVD_EFCTV_DATE'])}}   as  UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PROVIDER_CERTIFICATION_STATUS.sql:
  37: DISTINCT {{dbt_utils.generate_surrogate_key ( ['CRTF_STS_TYPE_CODE','PRVDR_STS_RSN_CODE'])}} as UNIQUE_ID_KEY, * FROM JOIN_PCS)

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PROVIDER_ENROLLMENT_STATUS_LOG.sql:
  309: {{dbt_utils.generate_surrogate_key ( ['PEACH_NUMBER', 'HIST_EFCTV_DATE'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PROVIDER_ENROLLMENT_STATUS.sql:
  39: DISTINCT {{dbt_utils.generate_surrogate_key ( ['ENRL_STS_TYPE_CODE','PRVDR_STS_RSN_CODE'])}} as UNIQUE_ID_KEY, * FROM JOIN_PES)

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_PROVIDER.sql:
  524:  {{dbt_utils.generate_surrogate_key ( ['PEACH_BASE_NUMBER','PEACH_SUFFIX_NUMBER'])}} as UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_RATING_ELEMENT.sql:
  37:  {{dbt_utils.generate_surrogate_key ( ['RT_ELEM_TYP_CD'])}} AS UNIQUE_ID_KEY  

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_RATING_PLAN_TYPE.sql:
  35: {{dbt_utils.generate_surrogate_key ( ['RT_ELEM_TYP_CD'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_REPRESENTATIVE.sql:
  252: {{ dbt_utils.generate_surrogate_key ( [ 'CUST_ID' ] ) }}   as  UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_REVENUE_CODE.sql:
  53:  {{dbt_utils.generate_surrogate_key ( ['SERVICE_CODE'])}} as UNIQUE_ID_KEY	,SERVICE_CODE

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_USER.sql:
  85: {{dbt_utils.generate_surrogate_key ( ['USER_LGN_NM'])}} as UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_WRITTEN_PREMIUM_ELEMENT.sql:
  14: {{dbt_utils.generate_surrogate_key ( ['WC_CLS_CLS_CD','WC_CLS_SUFX_CLS_SUFX','WC_CLS_SUFX_EFF_DT'])}}   as  UNIQUE_ID_KEY
  24: {{dbt_utils.generate_surrogate_key ( ['RT_ELEM_TYP_CD','RT_ELEM_TYP_NM_EFF_DT'])}}                as  UNIQUE_ID_KEY 

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DST_YEAR_CONTROL_ELEMENT.sql:
  44: {{dbt_utils.generate_surrogate_key ( ['POLICY_TYPE_CODE','PAYMENT_PLAN_TYPE_CODE','LEASE_TYPE_CODE'])}} AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DSV_ACTIVITY_DETAIL.sql:
  14: {{ dbt_utils.generate_surrogate_key ( [ 'ACTV_DTL_DESC' ] ) }}  as                    UNIQUE_ID_KEY                

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DSV_ACTIVITY.sql:
  13: {{ dbt_utils.generate_surrogate_key ( [ 'ACTV_ACTN_TYP_NM','ACTV_NM_TYP_NM','CNTX_TYP_NM','ACTV_DTL_COL_NM','SUBLOC_TYP_NM','FNCT_ROLE_NM' ] ) }}                                      as                                  UNIQUE_ID_KEY                    

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DSV_CLAIM_ADDITIONAL_ALLOWANCE.sql:
  158: {{ dbt_utils.generate_surrogate_key ( [ 'CLAIM_NUMBER', 'DOCUMENT_NUMBER', 'ALLOWANCE_REQUEST_FORM_CODE', 'FILED_DATE', 'ICD_CODE', 'CLAIM_ICD_DESC','CLAIM_ICD_LOCATION_CODE','CLAIM_ICD_SITE_CODE' ] ) }}  AS  UNIQUE_ID_KEY, * FROM  ETL

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DSV_CLAIM_PAYMENT_SUMMARY.sql:
  75:  {{ dbt_utils.generate_surrogate_key ( ['CLAIM_NUMBER'])}}  AS UNIQUE_ID_KEY

repo-dbt-engineering • dbt_root\projects\repo-dbt-snowflake\models\STAGING\DSV_EXAM_CASE_DETAIL.sql:
  52: {{ dbt_utils.generate_surrogate_key ( ['EXAM_REQUEST_TYPE_CODE','PRIMARY_PROVIDER_SPECIALTY_TYPE_CODE','SECONDARY_PROVIDER_SPECIALTY_TYPE_CODE','ADDENDUM_REQUEST_TYPE_CODE','LANGUAGE_TYPE_CODE'])}} AS UNIQUE_ID_KEY
