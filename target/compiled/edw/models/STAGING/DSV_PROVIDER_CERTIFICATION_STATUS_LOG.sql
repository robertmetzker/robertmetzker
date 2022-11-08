

---- SRC LAYER ----
WITH
SRC_PCS as ( SELECT *     from     STAGING.DST_PROVIDER_CERTIFICATION_STATUS_LOG ),
--SRC_PCS as ( SELECT *     from     DST_PROVIDER_CERTIFICATION_STATUS_LOG) ,

---- LOGIC LAYER ----

LOGIC_PCS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, PEACH_NUMBER                                       as                                       PEACH_NUMBER 
		, CRTF_STS_TYPE_CODE                                 as                                 CRTF_STS_TYPE_CODE 
		, CRTF_STS_TYPE_NAME                                 as                                 CRTF_STS_TYPE_NAME 
		, CRTF_STS_RSN_CODE                                  as                                  CRTF_STS_RSN_CODE 
		, CRTF_STS_RSN_NAME                                  as                                  CRTF_STS_RSN_NAME 
		, STS_EFCTV_DATE                                     as                                     STS_EFCTV_DATE 
		, STS_ENDNG_DATE                                     as                                     STS_ENDNG_DATE 
		, DRVD_EFCTV_DATE                                    as                                    DRVD_EFCTV_DATE 
		, DRVD_ENDNG_DATE                                    as                                    DRVD_ENDNG_DATE 
		, DRVD_EFCTV_USER_CODE                               as                               DRVD_EFCTV_USER_CODE 
		, DRVD_ENDNG_USER_CODE                               as                               DRVD_ENDNG_USER_CODE 
		, CASE WHEN CRTF_STS_TYPE_CODE = 'CMBND' and DRVD_ENDNG_DATE is null then 'Y' else 'N'
                                                            end as                                          PROVIDER_COMBINED_IND 
		from SRC_PCS
            )

---- RENAME LAYER ----
,

RENAME_PCS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, PEACH_NUMBER                                       as                              PROVIDER_PEACH_NUMBER
		, CRTF_STS_TYPE_CODE                                 as                      BWC_CERTIFICATION_STATUS_CODE
		, CRTF_STS_TYPE_NAME                                 as                      BWC_CERTIFICATION_STATUS_DESC
		, CRTF_STS_RSN_CODE                                  as               BWC_CERTIFICATION_STATUS_REASON_CODE
		, CRTF_STS_RSN_NAME                                  as               BWC_CERTIFICATION_STATUS_REASON_DESC
		, STS_EFCTV_DATE                                     as                       CERTIFICATION_EFFECTIVE_DATE
		, STS_ENDNG_DATE                                     as                             CERTIFICATION_END_DATE
		, DRVD_EFCTV_DATE                                    as                             DERIVED_EFFECTIVE_DATE
		, DRVD_ENDNG_DATE                                    as                                DERIVED_ENDING_DATE
		, DRVD_EFCTV_USER_CODE                               as                        DERIVED_EFFECTIVE_USER_CODE
		, DRVD_ENDNG_USER_CODE                               as                           DERIVED_ENDING_USER_CODE
		, PROVIDER_COMBINED_IND                                          as                              PROVIDER_COMBINED_IND 
				FROM     LOGIC_PCS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PCS                            as ( SELECT * from    RENAME_PCS   ),

---- JOIN LAYER ----

 JOIN_PCS  as  ( SELECT * 
				FROM  FILTER_PCS )
 SELECT * FROM  JOIN_PCS