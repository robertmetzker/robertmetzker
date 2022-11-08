---- SRC LAYER ----
WITH
SRC_PES as ( SELECT *     from     STAGING.STG_PROVIDER_ENROLLMENT_STATUS ),
//SRC_PES as ( SELECT *     from     STG_PROVIDER_ENROLLMENT_STATUS) ,

---- LOGIC LAYER ----

LOGIC_PES as ( SELECT 
		  TRIM( ENRL_STS_TYPE_CODE )                         as                                 ENRL_STS_TYPE_CODE 
		, TRIM( ENRL_STS_NAME )                              as                                      ENRL_STS_NAME 
		, TRIM( PRVDR_STS_RSN_CODE )                         as                                 PRVDR_STS_RSN_CODE 
		, TRIM( STS_RSN_TYPE_NAME )                          as                                  STS_RSN_TYPE_NAME 
		from SRC_PES
            )

---- RENAME LAYER ----
,

RENAME_PES as ( SELECT 
		  ENRL_STS_TYPE_CODE                                 as                                 ENRL_STS_TYPE_CODE
		, ENRL_STS_NAME                                      as                                      ENRL_STS_NAME
		, PRVDR_STS_RSN_CODE                                 as                                 PRVDR_STS_RSN_CODE
		, STS_RSN_TYPE_NAME                                  as                                  STS_RSN_TYPE_NAME 
				FROM     LOGIC_PES   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PES                            as ( SELECT * from    RENAME_PES   ),

---- JOIN LAYER ----

 JOIN_PES  as  ( SELECT * 
				FROM  FILTER_PES ),

------ ETL LAYER ----------------

ETL AS ( SELECT DISTINCT md5(cast(
    
    coalesce(cast(ENRL_STS_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(PRVDR_STS_RSN_CODE as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY, * FROM JOIN_PES)
SELECT * FROM ETL