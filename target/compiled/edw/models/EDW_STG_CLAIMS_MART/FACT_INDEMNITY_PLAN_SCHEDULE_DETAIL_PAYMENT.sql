

---- SRC LAYER ----
WITH
SRC_IPSD as ( SELECT *     from     STAGING.DSV_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT ),
SRC_C as ( SELECT *     from     EDW_STAGING_DIM.DIM_CUSTOMER ),
SRC_IW as ( SELECT *     from     EDW_STAGING_DIM.DIM_INJURED_WORKER ),
SRC_CLM as ( SELECT *     from     STAGING.DSV_CLAIM ),
//SRC_IPSD as ( SELECT *     from     DSV_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT) ,
//SRC_C as ( SELECT *     from     DIM_CUSTOMER) ,
//SRC_IW as ( SELECT *     from     DIM_INJURED_WORKER) ,
//SRC_CLM as ( SELECT *     from     DSV_CLAIM) ,

---- LOGIC LAYER ----

LOGIC_IPSD as ( SELECT 

		  INDEMNITY_PLAN_ID                                  as                                  INDEMNITY_PLAN_ID 
		, INDEMNITY_SCHEDULE_PAYMENT_DETAIL_ID               as               INDEMNITY_SCHEDULE_PAYMENT_DETAIL_ID 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( BENEFIT_TYPE_CODE, JURISDICTION_TYPE_CODE, BENEFIT_REPORTING_TYPE_DESC, INJURY_TYPE_CODE ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(BENEFIT_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(JURISDICTION_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(BENEFIT_REPORTING_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(INJURY_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                                  BENEFIT_TYPE_HKEY                                              
		, CASE WHEN  nullif(array_to_string(array_construct_compact( PTCP_TYP_CODE, CLM_PTCP_PRI_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(PTCP_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLM_PTCP_PRI_IND as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                      PAYEE_PARTICIPATION_TYPE_HKEY                                               
		, CASE WHEN INDM_PAY_EFF_DATE is null then '-1' 
			WHEN INDM_PAY_EFF_DATE < '1901-01-01' then '-2' 
			WHEN INDM_PAY_EFF_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( INDM_PAY_EFF_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                  INDM_PAY_EFF_DATE 
		, CASE WHEN INDM_PAY_END_DATE is null then '-1' 
			WHEN INDM_PAY_END_DATE < '1901-01-01' then '-2' 
			WHEN INDM_PAY_END_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( INDM_PAY_END_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                  INDM_PAY_END_DATE 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( INDM_FREQ_TYP_CODE, INDM_RSN_TYP_CODE, INDM_SCH_DTL_AMT_TYP_CODE, INDM_SCH_DTL_STS_TYP_CODE, INDM_SCH_DTL_FNL_PAY_IND, INDM_SCH_AUTO_PAY_IND, INDM_PAY_RECALC_IND, INDM_SCH_DTL_AMT_PRI_IND, INDM_SCH_DTL_AMT_MAILTO_IND, INDM_SCH_DTL_AMT_RMND_IND, OVR_PYMNT_BAL_IND, IP_VOID_IND, ISS_VOID_IND, ISD_VOID_IND, ISDA_VOID_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(INDM_FREQ_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(INDM_RSN_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_AMT_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_STS_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_FNL_PAY_IND as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_AUTO_PAY_IND as 
    varchar
), '') || '-' || coalesce(cast(INDM_PAY_RECALC_IND as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_AMT_PRI_IND as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_AMT_MAILTO_IND as 
    varchar
), '') || '-' || coalesce(cast(INDM_SCH_DTL_AMT_RMND_IND as 
    varchar
), '') || '-' || coalesce(cast(OVR_PYMNT_BAL_IND as 
    varchar
), '') || '-' || coalesce(cast(IP_VOID_IND as 
    varchar
), '') || '-' || coalesce(cast(ISS_VOID_IND as 
    varchar
), '') || '-' || coalesce(cast(ISD_VOID_IND as 
    varchar
), '') || '-' || coalesce(cast(ISDA_VOID_IND as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                   INDEMNITY_PLAN_SCHEDULE_DETAIL_HKEY                                              
		, CASE WHEN INDMN_PLAN_CREATE_USER_LGN_NAME is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(INDMN_PLAN_CREATE_USER_LGN_NAME as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                                 SCHEDULER_PLANNER_HKEY                                           
		, CASE WHEN CLAIM_AUTH_USER_LGN_NAME is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(CLAIM_AUTH_USER_LGN_NAME as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                            PAYMENT_AUTHORIZED_USER_HKEY                                        
		, CASE WHEN INDM_SCH_FST_PRCS_DATE is null then '-1' 
			WHEN INDM_SCH_FST_PRCS_DATE < '1901-01-01' then '-2' 
			WHEN INDM_SCH_FST_PRCS_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( INDM_SCH_FST_PRCS_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                             INDM_SCH_FST_PRCS_DATE 
		, CASE WHEN INDM_SCH_DATEL_DRV_EFF_DATE is null then '-1' 
			WHEN INDM_SCH_DATEL_DRV_EFF_DATE < '1901-01-01' then '-2' 
			WHEN INDM_SCH_DATEL_DRV_EFF_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( INDM_SCH_DATEL_DRV_EFF_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                        INDM_SCH_DATEL_DRV_EFF_DATE 
		, CASE WHEN INDM_SCH_DATEL_DRV_END_DATE is null then '-1' 
			WHEN INDM_SCH_DATEL_DRV_END_DATE < '1901-01-01' then '-2' 
			WHEN INDM_SCH_DATEL_DRV_END_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( INDM_SCH_DATEL_DRV_END_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                        INDM_SCH_DATEL_DRV_END_DATE 
		, CASE WHEN INDM_SCH_DLVR_DATE is null then '-1' 
			WHEN INDM_SCH_DLVR_DATE < '1901-01-01' then '-2' 
			WHEN INDM_SCH_DLVR_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( INDM_SCH_DLVR_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                 INDM_SCH_DLVR_DATE 
		, CASE WHEN INDM_SCH_DATEL_PRCS_DATE is null then '-1' 
			WHEN INDM_SCH_DATEL_PRCS_DATE < '1901-01-01' then '-2' 
			WHEN INDM_SCH_DATEL_PRCS_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( INDM_SCH_DATEL_PRCS_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                           INDM_SCH_DATEL_PRCS_DATE 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( PAY_MEDA_PREF_TYP_CODE, PAY_REQS_TYP_CODE, PAY_REQS_STT_TYP_CODE, PAY_REQS_STS_TYP_CODE, PAY_REQS_STS_RSN_TYP_CODE ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(PAY_MEDA_PREF_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(PAY_REQS_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(PAY_REQS_STT_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(PAY_REQS_STS_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(PAY_REQS_STS_RSN_TYP_CODE as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                              PAYMENT_REQUEST_HKEY                           
		, PAYEE_CHECK_GROUP_NUMBER                           as                           PAYEE_CHECK_GROUP_NUMBER 
		, CHILD_SUPPORT_CASE_NUMBER                          as                          CHILD_SUPPORT_CASE_NUMBER 
		, CHILD_SUPPORT_WITHHOLD_AMOUNT                      as                      CHILD_SUPPORT_WITHHOLD_AMOUNT 
		, INDEMNITY_PLAN_TOTAL_AMOUNT                        as                        INDEMNITY_PLAN_TOTAL_AMOUNT 
		, INDEMNITY_SCHEDULED_TOTAL_AMOUNT                   as                   INDEMNITY_SCHEDULED_TOTAL_AMOUNT 
       , SUM(IFF( IP_VOID_IND = 'N' AND ISS_VOID_IND = 'N' AND ISD_VOID_IND = 'N' AND ISDA_VOID_IND = 'N'
          AND INDM_SCH_DTL_AMT_TYP_CODE = 'BNFT' AND INDM_SCH_DTL_STS_TYP_CODE = 'PAID' , SCHEDULE_LINE_ITEM_DETAIL_AMOUNT,0)) 
		  over ( partition by CLAIM_NUMBER, INDEMNITY_PLAN_ID )
                                                             as                   INDEMNITY_PLAN_TOTAL_PAID_AMOUNT                                        
		, SCHEDULE_PAYMENT_COUNT                             as                             SCHEDULE_PAYMENT_COUNT 
		, SCHEDULE_DETAIL_DERIVED_DAY_COUNT                  as                  SCHEDULE_DETAIL_DERIVED_DAY_COUNT 
		, SCHEDULE_DETAIL_AMOUNT                             as                             SCHEDULE_DETAIL_AMOUNT 
		, SCHEDULE_DETAIL_OFFSET_AMOUNT                      as                      SCHEDULE_DETAIL_OFFSET_AMOUNT 
		, LUMPSUM_REDUCTION_AMOUNT                           as                           LUMPSUM_REDUCTION_AMOUNT 
		, OTHER_REDUCTION_AMOUNT                             as                             OTHER_REDUCTION_AMOUNT 
		, SCHEDULE_DETAIL_NET_AMOUNT                         as                         SCHEDULE_DETAIL_NET_AMOUNT 
		, SCHEDULE_LINE_ITEM_DETAIL_AMOUNT                   as                   SCHEDULE_LINE_ITEM_DETAIL_AMOUNT 
		, PERMANENT_PARTIAL_PERCENT                          as                          PERMANENT_PARTIAL_PERCENT 
		, INDEMNITY_BENEFIT_PLAN_WEEK_COUNT                  as                  INDEMNITY_BENEFIT_PLAN_WEEK_COUNT 
		, INDEMNITY_BENEFIT_PLAN_DAY_COUNT                   as                   INDEMNITY_BENEFIT_PLAN_DAY_COUNT 
		, DISPUTED_TRANSACTION_DAY_COUNT                     as                     DISPUTED_TRANSACTION_DAY_COUNT 
		, CUST_NO                                            as                                            CUST_NO 
		, BENEFIT_TYPE_CODE                                  as                                  BENEFIT_TYPE_CODE 
		, JURISDICTION_TYPE_CODE                             as                             JURISDICTION_TYPE_CODE 
		, BNFT_RPT_TYP_CODE                                  as                                  BNFT_RPT_TYP_CODE 
		, PTCP_TYP_CODE                                      as                                      PTCP_TYP_CODE 
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND 
		, INDM_FREQ_TYP_CODE                                 as                                 INDM_FREQ_TYP_CODE 
		, INDM_RSN_TYP_CODE                                  as                                  INDM_RSN_TYP_CODE 
		, INDM_SCH_DTL_AMT_TYP_CODE                          as                          INDM_SCH_DTL_AMT_TYP_CODE 
		, INDM_SCH_DTL_STS_TYP_CODE                          as                          INDM_SCH_DTL_STS_TYP_CODE 
		, INDM_SCH_DTL_FNL_PAY_IND                           as                           INDM_SCH_DTL_FNL_PAY_IND 
		, INDM_SCH_AUTO_PAY_IND                              as                              INDM_SCH_AUTO_PAY_IND 
		, INDM_PAY_RECALC_IND                                as                                INDM_PAY_RECALC_IND 
		, INDM_SCH_DTL_AMT_PRI_IND                           as                           INDM_SCH_DTL_AMT_PRI_IND 
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                        INDM_SCH_DTL_AMT_MAILTO_IND 
		, INDM_SCH_DTL_AMT_RMND_IND                          as                          INDM_SCH_DTL_AMT_RMND_IND 
		, OVR_PYMNT_BAL_IND                                  as                                  OVR_PYMNT_BAL_IND 
		, IP_VOID_IND                                        as                                        IP_VOID_IND 
		, ISS_VOID_IND                                       as                                       ISS_VOID_IND 
		, ISD_VOID_IND                                       as                                       ISD_VOID_IND 
		, ISDA_VOID_IND                                      as                                      ISDA_VOID_IND 
		, CLAIM_AUTH_USER_LGN_NAME                           as                             CLAIM_AUTH_USER_LGN_NM 
		, PAY_MEDA_PREF_TYP_CODE                             as                             PAY_MEDA_PREF_TYP_CODE 
		, PAY_REQS_TYP_CODE                                  as                                  PAY_REQS_TYP_CODE 
		, PAY_REQS_STT_TYP_CODE                              as                              PAY_REQS_STT_TYP_CODE 
		, PAY_REQS_STS_TYP_CODE                              as                              PAY_REQS_STS_TYP_CODE 
		, PAY_REQS_STS_RSN_TYP_CODE                          as                          PAY_REQS_STS_RSN_TYP_CODE 
		, INDMN_PLAN_CREATE_USER_LGN_NAME                    as                    INDMN_PLAN_CREATE_USER_LGN_NAME 
        , CASE WHEN WARRANT_DATE is null then '-1' 
			WHEN WARRANT_DATE < '1901-01-01' then '-2' 
			WHEN WARRANT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( WARRANT_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                       WARRANT_DATE 
		, WARRANT_NUMBER                                     as                                     WARRANT_NUMBER 			
		from SRC_IPSD
            ),
LOGIC_C as ( SELECT 
		  COALESCE( CUSTOMER_HKEY, MD5( '99999' ) )          as                                     CUSTOMER_HKEY 
		, CUSTOMER_NUMBER                                    as                                   CUSTOMER_NUMBER  
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 

		from SRC_C
		where CUSTOMER_NUMBER||PRIMARY_SOURCE_SYSTEM != '99999.00000CAM'    -- Need to revisit this specific logic and need to remove when DIM_CUSTOMER is fixed
            ),
LOGIC_IW as ( SELECT 
		   COALESCE( INJURED_WORKER_HKEY, MD5( '99999' ) )    as                                INJURED_WORKER_HKEY 
		 , CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER  
		 , RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
         , RECORD_END_DATE                                    as                                    RECORD_END_DATE
		from SRC_IW
            ),
LOGIC_CLM as ( SELECT 
        IW_CUSTOMER_NUMBER                                   as                                 IW_CUSTOMER_NUMBER
		,  CASE WHEN  nullif(array_to_string(array_construct_compact( CURRENT_CORESUITE_CLAIM_TYPE_CODE, CLAIM_TYPE_CHNG_OVR_IND, CLAIM_STATE_CODE, CLAIM_STATUS_CODE, CLAIM_STATUS_REASON_CODE ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(CURRENT_CORESUITE_CLAIM_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_TYPE_CHNG_OVR_IND as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATUS_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATUS_REASON_CODE as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                            CLAIM_TYPE_STATUS_HKEY                                              
		, CASE WHEN  nullif(array_to_string(array_construct_compact( FILING_SOURCE_DESC, FILING_MEDIA_DESC, NATURE_OF_INJURY_CATEGORY, NATURE_OF_INJURY_TYPE, FIREFIGHTER_CANCER_IND, COVID_EXPOSURE_IND, COVID_EMERGENCY_WORKER_IND, COVID_HEALTH_CARE_WORKER_IND, COMBINED_IND, SB223_IND, EMPLOYER_PREMISES_IND, CATASTROPHIC_IND, K_PROGRAM_ENROLLMENT_DESC, K_PROGRAM_TYPE_DESC, K_PROGRAM_REASON_DESC ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(FILING_SOURCE_DESC as 
    varchar
), '') || '-' || coalesce(cast(FILING_MEDIA_DESC as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_CATEGORY as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_TYPE as 
    varchar
), '') || '-' || coalesce(cast(FIREFIGHTER_CANCER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EXPOSURE_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EMERGENCY_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_HEALTH_CARE_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COMBINED_IND as 
    varchar
), '') || '-' || coalesce(cast(SB223_IND as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYER_PREMISES_IND as 
    varchar
), '') || '-' || coalesce(cast(CATASTROPHIC_IND as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_ENROLLMENT_DESC as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_REASON_DESC as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                                  CLAIM_DETAIL_HKEY  
														                                  
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, CURRENT_CORESUITE_CLAIM_TYPE_CODE                  as                  CURRENT_CORESUITE_CLAIM_TYPE_CODE 
		, CLAIM_TYPE_CHNG_OVR_IND                            as                            CLAIM_TYPE_CHNG_OVR_IND 
		, CLAIM_STATE_CODE                                   as                                   CLAIM_STATE_CODE 
		, CLAIM_STATUS_CODE                                  as                                  CLAIM_STATUS_CODE 
		, CLAIM_STATUS_REASON_CODE                           as                           CLAIM_STATUS_REASON_CODE 
		, FILING_SOURCE_DESC                                 as                                 FILING_SOURCE_DESC 
		, FILING_MEDIA_DESC                                  as                                  FILING_MEDIA_DESC 
		, NATURE_OF_INJURY_CATEGORY                          as                          NATURE_OF_INJURY_CATEGORY 
		, NATURE_OF_INJURY_TYPE                              as                              NATURE_OF_INJURY_TYPE 
		, FIREFIGHTER_CANCER_IND                             as                             FIREFIGHTER_CANCER_IND 
		, COVID_EXPOSURE_IND                                 as                                 COVID_EXPOSURE_IND 
		, COVID_EMERGENCY_WORKER_IND                         as                         COVID_EMERGENCY_WORKER_IND 
		, COVID_HEALTH_CARE_WORKER_IND                       as                       COVID_HEALTH_CARE_WORKER_IND 
		, COMBINED_IND                                       as                                       COMBINED_IND 
		, SB223_IND                                          as                                          SB223_IND 
		, EMPLOYER_PREMISES_IND                              as                              EMPLOYER_PREMISES_IND 
		, CATASTROPHIC_IND                                   as                                   CATASTROPHIC_IND 
		, K_PROGRAM_ENROLLMENT_DESC                          as                          K_PROGRAM_ENROLLMENT_DESC 
		, K_PROGRAM_TYPE_DESC                                as                                K_PROGRAM_TYPE_DESC 
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC 
		from SRC_CLM
            )

---- RENAME LAYER ----
,

RENAME_IPSD as ( SELECT 
		  INDEMNITY_PLAN_ID                                  as                                  INDEMNITY_PLAN_ID
		, INDEMNITY_SCHEDULE_PAYMENT_DETAIL_ID               as               INDEMNITY_SCHEDULE_PAYMENT_DETAIL_ID
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, BENEFIT_TYPE_HKEY                                  as                                  BENEFIT_TYPE_HKEY
		, PAYEE_PARTICIPATION_TYPE_HKEY                      as                      PAYEE_PARTICIPATION_TYPE_HKEY
		, INDM_PAY_EFF_DATE                                  as               INDEMNITY_BENEFIT_EFFECTIVE_DATE_KEY
		, INDM_PAY_END_DATE                                  as                     INDEMNITY_BENEFIT_END_DATE_KEY
		, INDEMNITY_PLAN_SCHEDULE_DETAIL_HKEY                as                INDEMNITY_PLAN_SCHEDULE_DETAIL_HKEY
		, SCHEDULER_PLANNER_HKEY                             as                             SCHEDULER_PLANNER_HKEY
		, PAYMENT_AUTHORIZED_USER_HKEY                       as                       PAYMENT_AUTHORIZED_USER_HKEY
		, INDM_SCH_FST_PRCS_DATE                             as                    SCHEDULE_FIRST_PROCESS_DATE_KEY
		, INDM_SCH_DATEL_DRV_EFF_DATE                        as                 SCHEDULE_DETAIL_EFFECTIVE_DATE_KEY
		, INDM_SCH_DATEL_DRV_END_DATE                        as                       SCHEDULE_DETAIL_END_DATE_KEY
		, INDM_SCH_DLVR_DATE                                 as                  SCHEDULE_DETAIL_DELIVERY_DATE_KEY
		, INDM_SCH_DATEL_PRCS_DATE                           as                   SCHEDULE_DETAIL_PROCESS_DATE_KEY
		, PAYMENT_REQUEST_HKEY                               as                               PAYMENT_REQUEST_HKEY
		, PAYEE_CHECK_GROUP_NUMBER                           as                           PAYEE_CHECK_GROUP_NUMBER
		, CHILD_SUPPORT_CASE_NUMBER                          as                          CHILD_SUPPORT_CASE_NUMBER
		, CHILD_SUPPORT_WITHHOLD_AMOUNT                      as                      CHILD_SUPPORT_WITHHOLD_AMOUNT
		, INDEMNITY_PLAN_TOTAL_AMOUNT                        as                        INDEMNITY_PLAN_TOTAL_AMOUNT
		, INDEMNITY_SCHEDULED_TOTAL_AMOUNT                   as                   INDEMNITY_SCHEDULED_TOTAL_AMOUNT		
		, INDEMNITY_PLAN_TOTAL_PAID_AMOUNT                   as                   INDEMNITY_PLAN_TOTAL_PAID_AMOUNT
		, SCHEDULE_PAYMENT_COUNT                             as                             SCHEDULE_PAYMENT_COUNT
		, SCHEDULE_DETAIL_DERIVED_DAY_COUNT                  as                  SCHEDULE_DETAIL_DERIVED_DAY_COUNT
		, SCHEDULE_DETAIL_AMOUNT                             as                       SCHEDULE_DETAIL_GROSS_AMOUNT
		, SCHEDULE_DETAIL_OFFSET_AMOUNT                      as                      SCHEDULE_DETAIL_OFFSET_AMOUNT
		, LUMPSUM_REDUCTION_AMOUNT                           as                           LUMPSUM_REDUCTION_AMOUNT
		, OTHER_REDUCTION_AMOUNT                             as                             OTHER_REDUCTION_AMOUNT
		, SCHEDULE_DETAIL_NET_AMOUNT                         as                         SCHEDULE_DETAIL_NET_AMOUNT
		, SCHEDULE_LINE_ITEM_DETAIL_AMOUNT                   as                   SCHEDULE_LINE_ITEM_DETAIL_AMOUNT
		, PERMANENT_PARTIAL_PERCENT                          as                          PERMANENT_PARTIAL_PERCENT
		, INDEMNITY_BENEFIT_PLAN_WEEK_COUNT                  as                  INDEMNITY_BENEFIT_PLAN_WEEK_COUNT
		, INDEMNITY_BENEFIT_PLAN_DAY_COUNT                   as                   INDEMNITY_BENEFIT_PLAN_DAY_COUNT
		, DISPUTED_TRANSACTION_DAY_COUNT                     as                     DISPUTED_TRANSACTION_DAY_COUNT
		, CUST_NO                                            as                                            CUST_NO
		, BENEFIT_TYPE_CODE                                  as                                  BENEFIT_TYPE_CODE
		, JURISDICTION_TYPE_CODE                             as                             JURISDICTION_TYPE_CODE
		, BNFT_RPT_TYP_CODE                                  as                                  BNFT_RPT_TYP_CODE
		, PTCP_TYP_CODE                                      as                                      PTCP_TYP_CODE
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND
		, INDM_FREQ_TYP_CODE                                 as                                 INDM_FREQ_TYP_CODE
		, INDM_RSN_TYP_CODE                                  as                                  INDM_RSN_TYP_CODE
		, INDM_SCH_DTL_AMT_TYP_CODE                          as                          INDM_SCH_DTL_AMT_TYP_CODE
		, INDM_SCH_DTL_STS_TYP_CODE                          as                          INDM_SCH_DTL_STS_TYP_CODE
		, INDM_SCH_DTL_FNL_PAY_IND                           as                           INDM_SCH_DTL_FNL_PAY_IND
		, INDM_SCH_AUTO_PAY_IND                              as                              INDM_SCH_AUTO_PAY_IND
		, INDM_PAY_RECALC_IND                                as                                INDM_PAY_RECALC_IND
		, INDM_SCH_DTL_AMT_PRI_IND                           as                           INDM_SCH_DTL_AMT_PRI_IND
		, INDM_SCH_DTL_AMT_MAILTO_IND                        as                        INDM_SCH_DTL_AMT_MAILTO_IND
		, INDM_SCH_DTL_AMT_RMND_IND                          as                          INDM_SCH_DTL_AMT_RMND_IND
		, OVR_PYMNT_BAL_IND                                  as                                  OVR_PYMNT_BAL_IND
		, IP_VOID_IND                                        as                                        IP_VOID_IND
		, ISS_VOID_IND                                       as                                       ISS_VOID_IND
		, ISD_VOID_IND                                       as                                       ISD_VOID_IND
		, ISDA_VOID_IND                                      as                                      ISDA_VOID_IND
		, CLAIM_AUTH_USER_LGN_NM                             as                             CLAIM_AUTH_USER_LGN_NM
		, PAY_MEDA_PREF_TYP_CODE                             as                             PAY_MEDA_PREF_TYP_CODE
		, PAY_REQS_TYP_CODE                                  as                                  PAY_REQS_TYP_CODE
		, PAY_REQS_STT_TYP_CODE                              as                              PAY_REQS_STT_TYP_CODE
		, PAY_REQS_STS_TYP_CODE                              as                              PAY_REQS_STS_TYP_CODE
		, PAY_REQS_STS_RSN_TYP_CODE                          as                          PAY_REQS_STS_RSN_TYP_CODE
		, INDMN_PLAN_CREATE_USER_LGN_NAME                    as                    INDMN_PLAN_CREATE_USER_LGN_NAME 
		, WARRANT_DATE                                       as                                   WARRANT_DATE_KEY
		, WARRANT_NUMBER                                     as                                     WARRANT_NUMBER		
				FROM     LOGIC_IPSD   ), 
RENAME_C as ( SELECT 
		  CUSTOMER_HKEY                                      as                                         PAYEE_HKEY
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                            C_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                  C_RECORD_END_DATE 
				FROM     LOGIC_C   ), 
RENAME_IW as ( SELECT 
		  INJURED_WORKER_HKEY                                 as                               INJURED_WORKER_HKEY 
		 , CUSTOMER_NUMBER                                    as                                   CUSTOMER_NUMBER   
		 , RECORD_EFFECTIVE_DATE                              as                          IW_RECORD_EFFECTIVE_DATE
         , RECORD_END_DATE                                    as                                IW_RECORD_END_DATE
		 				FROM     LOGIC_IW   ), 
RENAME_CLM as ( SELECT 
          IW_CUSTOMER_NUMBER                                 as                                 IW_CUSTOMER_NUMBER
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, CLAIM_NUMBER                                       as                                   CLM_CLAIM_NUMBER
		, CURRENT_CORESUITE_CLAIM_TYPE_CODE                  as                  CURRENT_CORESUITE_CLAIM_TYPE_CODE
		, CLAIM_TYPE_CHNG_OVR_IND                            as                            CLAIM_TYPE_CHNG_OVR_IND
		, CLAIM_STATE_CODE                                   as                                   CLAIM_STATE_CODE
		, CLAIM_STATUS_CODE                                  as                                  CLAIM_STATUS_CODE
		, CLAIM_STATUS_REASON_CODE                           as                           CLAIM_STATUS_REASON_CODE
		, FILING_SOURCE_DESC                                 as                                 FILING_SOURCE_DESC
		, FILING_MEDIA_DESC                                  as                                  FILING_MEDIA_DESC
		, NATURE_OF_INJURY_CATEGORY                          as                          NATURE_OF_INJURY_CATEGORY
		, NATURE_OF_INJURY_TYPE                              as                              NATURE_OF_INJURY_TYPE
		, FIREFIGHTER_CANCER_IND                             as                             FIREFIGHTER_CANCER_IND
		, COVID_EXPOSURE_IND                                 as                                 COVID_EXPOSURE_IND
		, COVID_EMERGENCY_WORKER_IND                         as                         COVID_EMERGENCY_WORKER_IND
		, COVID_HEALTH_CARE_WORKER_IND                       as                       COVID_HEALTH_CARE_WORKER_IND
		, COMBINED_IND                                       as                                       COMBINED_IND
		, SB223_IND                                          as                                          SB223_IND
		, EMPLOYER_PREMISES_IND                              as                              EMPLOYER_PREMISES_IND
		, CATASTROPHIC_IND                                   as                                   CATASTROPHIC_IND
		, K_PROGRAM_ENROLLMENT_DESC                          as                          K_PROGRAM_ENROLLMENT_DESC
		, K_PROGRAM_TYPE_DESC                                as                                K_PROGRAM_TYPE_DESC
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC 
				FROM     LOGIC_CLM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_IPSD                           as ( SELECT * from    RENAME_IPSD   ),
FILTER_C                              as ( SELECT * from    RENAME_C   ),
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_IW                             as ( SELECT * from    RENAME_IW   ),

---- JOIN LAYER ----

CLM as ( SELECT * 
				FROM  FILTER_IPSD
				LEFT JOIN FILTER_CLM  ON  FILTER_IPSD.CLAIM_NUMBER =  FILTER_CLM.CLM_CLAIM_NUMBER 
						LEFT JOIN FILTER_C ON  NVL(FILTER_IPSD.CUST_NO,'00000') =  FILTER_C.CUSTOMER_NUMBER AND  CURRENT_DATE  BETWEEN C_RECORD_EFFECTIVE_DATE AND coalesce( C_RECORD_END_DATE, '2099-12-31') 					
						LEFT JOIN FILTER_IW ON  coalesce( FILTER_CLM.IW_CUSTOMER_NUMBER, '99999') =  FILTER_IW.CUSTOMER_NUMBER AND CURRENT_DATE BETWEEN IW_RECORD_EFFECTIVE_DATE AND coalesce( IW_RECORD_END_DATE, '2099-12-31') 						
						)
SELECT 
		  INDEMNITY_PLAN_ID
		, INDEMNITY_SCHEDULE_PAYMENT_DETAIL_ID
		, CLAIM_NUMBER
		, coalesce( BENEFIT_TYPE_HKEY, MD5( '99999' )) as BENEFIT_TYPE_HKEY
		, coalesce( PAYEE_HKEY, MD5( '99999' )) as PAYEE_HKEY 
		, coalesce( PAYEE_PARTICIPATION_TYPE_HKEY, MD5( '99999' )) as PAYEE_PARTICIPATION_TYPE_HKEY
		, coalesce( INJURED_WORKER_HKEY, MD5( '-1111' )) as INJURED_WORKER_HKEY
		, INDEMNITY_BENEFIT_EFFECTIVE_DATE_KEY
		, INDEMNITY_BENEFIT_END_DATE_KEY
		, coalesce( INDEMNITY_PLAN_SCHEDULE_DETAIL_HKEY, MD5( '99999' )) as INDEMNITY_PLAN_SCHEDULE_DETAIL_HKEY
		, coalesce( SCHEDULER_PLANNER_HKEY, MD5( '99999' )) as SCHEDULER_PLANNER_HKEY
		, coalesce( PAYMENT_AUTHORIZED_USER_HKEY, MD5( '99999' )) as PAYMENT_AUTHORIZED_USER_HKEY
		, SCHEDULE_FIRST_PROCESS_DATE_KEY
		, SCHEDULE_DETAIL_EFFECTIVE_DATE_KEY
		, SCHEDULE_DETAIL_END_DATE_KEY
		, SCHEDULE_DETAIL_DELIVERY_DATE_KEY
		, SCHEDULE_DETAIL_PROCESS_DATE_KEY
		, coalesce( PAYMENT_REQUEST_HKEY, MD5( '99999' )) as PAYMENT_REQUEST_HKEY
		, coalesce( CLAIM_TYPE_STATUS_HKEY, MD5( '99999' )) as CLAIM_TYPE_STATUS_HKEY
		, coalesce( CLAIM_DETAIL_HKEY, MD5( '99999' )) as CLAIM_DETAIL_HKEY
		, WARRANT_DATE_KEY
		, WARRANT_NUMBER
		, PAYEE_CHECK_GROUP_NUMBER
		, CHILD_SUPPORT_CASE_NUMBER
		, CHILD_SUPPORT_WITHHOLD_AMOUNT
		, INDEMNITY_PLAN_TOTAL_AMOUNT
		, INDEMNITY_SCHEDULED_TOTAL_AMOUNT
		, INDEMNITY_PLAN_TOTAL_PAID_AMOUNT
		, SCHEDULE_PAYMENT_COUNT
		, SCHEDULE_DETAIL_DERIVED_DAY_COUNT
		, SCHEDULE_DETAIL_GROSS_AMOUNT
		, SCHEDULE_DETAIL_OFFSET_AMOUNT
		, LUMPSUM_REDUCTION_AMOUNT
		, OTHER_REDUCTION_AMOUNT
		, SCHEDULE_DETAIL_NET_AMOUNT
		, SCHEDULE_LINE_ITEM_DETAIL_AMOUNT
		, cast(PERMANENT_PARTIAL_PERCENT as NUMERIC(15,4)) AS PERMANENT_PARTIAL_PERCENT
		, INDEMNITY_BENEFIT_PLAN_WEEK_COUNT
		, INDEMNITY_BENEFIT_PLAN_DAY_COUNT
		, DISPUTED_TRANSACTION_DAY_COUNT
		, CURRENT_TIMESTAMP AS LOAD_DATETIME
		, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
		, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 

from CLM