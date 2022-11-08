

---- SRC LAYER ----
WITH
SRC_PES as ( SELECT *     from     STAGING.DST_PROVIDER_ENROLLMENT_STATUS ),
//SRC_PES as ( SELECT *     from     DST_PROVIDER_ENROLLMENT_STATUS) ,

---- LOGIC LAYER ----

LOGIC_PES as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, ENRL_STS_TYPE_CODE                                 as                                 ENRL_STS_TYPE_CODE 
		, PRVDR_STS_RSN_CODE                                 as                                 PRVDR_STS_RSN_CODE 
		, ENRL_STS_NAME                                      as                                      ENRL_STS_NAME 
		, STS_RSN_TYPE_NAME                                  as                                  STS_RSN_TYPE_NAME 
		from SRC_PES
            )

---- RENAME LAYER ----
,

RENAME_PES as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, ENRL_STS_TYPE_CODE                                 as                        ENROLLMENT_STATUS_TYPE_CODE
		, PRVDR_STS_RSN_CODE                                 as                      ENROLLMENT_STATUS_REASON_CODE
		, ENRL_STS_NAME                                      as                        ENROLLMENT_STATUS_TYPE_DESC
		, STS_RSN_TYPE_NAME                                  as                      ENROLLMENT_STATUS_REASON_DESC 
				FROM     LOGIC_PES   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PES                            as ( SELECT * from    RENAME_PES   ),

---- JOIN LAYER ----

 JOIN_PES  as  ( SELECT * 
				FROM  FILTER_PES )
 SELECT * FROM  JOIN_PES