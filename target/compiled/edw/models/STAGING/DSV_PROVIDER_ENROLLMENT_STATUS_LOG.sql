

---- SRC LAYER ----
WITH
SRC_PES as ( SELECT *     from     STAGING.DST_PROVIDER_ENROLLMENT_STATUS_LOG ),
//SRC_PES as ( SELECT *     from     DST_PROVIDER_ENROLLMENT_STATUS_LOG) ,

---- LOGIC LAYER ----

LOGIC_PES as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, PEACH_NUMBER                                       AS                                       PEACH_NUMBER 
		, ENRL_STS_TYPE_CODE                                 AS                                 ENRL_STS_TYPE_CODE 
		, ENRL_STS_RSN_CODE                                  AS                                 PRVDR_STS_RSN_CODE 
		, ENRL_STS_TYPE_NAME                                 AS                                      ENRL_STS_NAME 
		, ENRL_STS_RSN_NAME                                  AS                                  STS_RSN_TYPE_NAME 
		, STS_EFCTV_DATE                                     AS                                     STS_EFCTV_DATE 
		, STS_ENDNG_DATE                                     AS                                     STS_ENDNG_DATE 
		, DRVD_EFCTV_DATE                                    AS                                    DRVD_EFCTV_DATE 
		, DRVD_ENDNG_DATE                                    AS                                    DRVD_ENDNG_DATE 
		, DRVD_EFCTV_USER_CODE                               AS                               DRVD_EFCTV_USER_CODE 
		, DRVD_ENDNG_USER_CODE                               AS                               DRVD_ENDNG_USER_CODE 
		from SRC_PES
            )

---- RENAME LAYER ----
,

RENAME_PES as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, PEACH_NUMBER                                       as                              PROVIDER_PEACH_NUMBER
		, ENRL_STS_TYPE_CODE                                 as                        ENROLLMENT_STATUS_TYPE_CODE
		, PRVDR_STS_RSN_CODE                                 as                      ENROLLMENT_STATUS_REASON_CODE
		, ENRL_STS_NAME                                      as                        ENROLLMENT_STATUS_TYPE_DESC
		, STS_RSN_TYPE_NAME                                  as                      ENROLLMENT_STATUS_REASON_DESC
		, STS_EFCTV_DATE                                     as                          ENROLLMENT_EFFECTIVE_DATE
		, STS_ENDNG_DATE                                     as                                ENROLLMENT_END_DATE
		, DRVD_EFCTV_DATE                                    as                             DERIVED_EFFECTIVE_DATE
		, DRVD_ENDNG_DATE                                    as                                DERIVED_ENDING_DATE
		, DRVD_EFCTV_USER_CODE                               as                        DERIVED_EFFECTIVE_USER_CODE
		, DRVD_ENDNG_USER_CODE                               as                           DERIVED_ENDING_USER_CODE 
				FROM     LOGIC_PES   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PES                            as ( SELECT * from    RENAME_PES   ),

---- JOIN LAYER ----

 JOIN_PES  as  ( SELECT * 
				FROM  FILTER_PES )
 SELECT * FROM  JOIN_PES