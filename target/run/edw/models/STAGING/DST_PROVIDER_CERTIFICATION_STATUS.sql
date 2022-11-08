

      create or replace  table DEV_EDW.STAGING.DST_PROVIDER_CERTIFICATION_STATUS  as
      (---- SRC LAYER ----
WITH
SRC_PCS as ( SELECT *     from     STAGING.STG_PROVIDER_CERTIFICATION_STATUS ),
//SRC_PCS as ( SELECT *     from     STG_PROVIDER_CERTIFICATION_STATUS) ,

---- LOGIC LAYER ----

LOGIC_PCS as ( SELECT 
		  TRIM( CRTF_STS_TYPE_CODE )                         as                                 CRTF_STS_TYPE_CODE 
		, TRIM( CRTF_STS_TYPE_NAME )                         as                                 CRTF_STS_TYPE_NAME 
		, TRIM( PRVDR_STS_RSN_CODE )                         as                                 PRVDR_STS_RSN_CODE 
		, TRIM( STS_RSN_TYPE_NAME )                          as                                  STS_RSN_TYPE_NAME 
		from SRC_PCS
            )

---- RENAME LAYER ----
,

RENAME_PCS as ( SELECT 
		  CRTF_STS_TYPE_CODE                                 as                                 CRTF_STS_TYPE_CODE
		, CRTF_STS_TYPE_NAME                                 as                                 CRTF_STS_TYPE_NAME
		, PRVDR_STS_RSN_CODE                                 as                                 PRVDR_STS_RSN_CODE
		, STS_RSN_TYPE_NAME                                  as                                  STS_RSN_TYPE_NAME 
				FROM     LOGIC_PCS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PCS                            as ( SELECT * from    RENAME_PCS   ),

---- JOIN LAYER ----

 JOIN_PCS  as  ( SELECT * 
				FROM  FILTER_PCS ),
----- ETL LAYER ----------------
ETL AS ( SELECT DISTINCT md5(cast(
    
    coalesce(cast(CRTF_STS_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(PRVDR_STS_RSN_CODE as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY, * FROM JOIN_PCS)
SELECT * FROM ETL
      );
    