
  create or replace  view DEV_EDW.STAGING.DSV_PROVIDER_CERTIFICATION_STATUS  as (
    

---- SRC LAYER ----
WITH
SRC_PCS as ( SELECT *     from     STAGING.DST_PROVIDER_CERTIFICATION_STATUS ),
//SRC_PCS as ( SELECT *     from     DST_PROVIDER_CERTIFICATION_STATUS) ,

---- LOGIC LAYER ----

LOGIC_PCS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CRTF_STS_TYPE_CODE                                 as                                 CRTF_STS_TYPE_CODE 
		, PRVDR_STS_RSN_CODE                                 as                                 PRVDR_STS_RSN_CODE 
		, CRTF_STS_TYPE_NAME                                 as                                 CRTF_STS_TYPE_NAME 
		, STS_RSN_TYPE_NAME                                  as                                  STS_RSN_TYPE_NAME 
		from SRC_PCS
            )

---- RENAME LAYER ----
,

RENAME_PCS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CRTF_STS_TYPE_CODE                                 as                      BWC_CERTIFICATION_STATUS_CODE
		, PRVDR_STS_RSN_CODE                                 as               BWC_CERTIFICATION_STATUS_REASON_CODE
		, CRTF_STS_TYPE_NAME                                 as                      BWC_CERTIFICATION_STATUS_DESC
		, STS_RSN_TYPE_NAME                                  as               BWC_CERTIFICATION_STATUS_REASON_DESC 
				FROM     LOGIC_PCS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PCS                            as ( SELECT * from    RENAME_PCS   ),

---- JOIN LAYER ----

 JOIN_PCS  as  ( SELECT * 
				FROM  FILTER_PCS )
 SELECT * FROM  JOIN_PCS
  );
