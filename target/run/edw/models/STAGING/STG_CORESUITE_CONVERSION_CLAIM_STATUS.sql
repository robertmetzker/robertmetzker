

      create or replace  table DEV_EDW.STAGING.STG_CORESUITE_CONVERSION_CLAIM_STATUS  as
      (---- SRC LAYER ----
WITH
SRC_CS as ( SELECT *     from     DEV_VIEWS.BWC_FILES.CORESUITE_CONVERSION_CLAIM_STATUS ),
//SRC_CS as ( SELECT *     from     CORESUITE_CONVERSION_CLAIM_STATUS) ,

---- LOGIC LAYER ----

LOGIC_CS as ( SELECT 
		  upper( TRIM( CLAIM_STATE_CODE ) )                  AS                                   CLAIM_STATE_CODE 
		, upper( TRIM( CLAIM_STATUS_CODE ) )                 AS                                  CLAIM_STATUS_CODE 
		, upper( TRIM( CLAIM_STATUS_REASON_CODE ) )          AS                           CLAIM_STATUS_REASON_CODE 
		, upper( TRIM( CLAIM_TYPE_CODE ) )                   AS                                    CLAIM_TYPE_CODE 
		, upper( TRIM( CLAIM_WEB_STATUS_CODE ) )             AS                              CLAIM_WEB_STATUS_CODE 
		, upper( TRIM( CLAIM_WEB_STATUS_DESC ) )             AS                              CLAIM_WEB_STATUS_DESC 
		, upper( TRIM( CLAIM_WEB_STATUS_COMMENT ) )          AS                           CLAIM_WEB_STATUS_COMMENT 
		from SRC_CS
            )

---- RENAME LAYER ----
,

RENAME_CS as ( SELECT 
		  CLAIM_STATE_CODE                                   as                                   CLAIM_STATE_CODE
		, CLAIM_STATUS_CODE                                  as                                  CLAIM_STATUS_CODE
		, CLAIM_STATUS_REASON_CODE                           as                           CLAIM_STATUS_REASON_CODE
		, CLAIM_TYPE_CODE                                    as                                    CLAIM_TYPE_CODE
		, CLAIM_WEB_STATUS_CODE                              as                              CLAIM_WEB_STATUS_CODE
		, CLAIM_WEB_STATUS_DESC                              as                              CLAIM_WEB_STATUS_DESC
		, CLAIM_WEB_STATUS_COMMENT                           as                           CLAIM_WEB_STATUS_COMMENT 
				FROM     LOGIC_CS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),

---- JOIN LAYER ----

 JOIN_CS  as  ( SELECT * 
				FROM  FILTER_CS )
 SELECT * FROM  JOIN_CS
      );
    