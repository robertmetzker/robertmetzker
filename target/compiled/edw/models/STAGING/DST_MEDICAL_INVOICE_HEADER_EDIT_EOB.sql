---- SRC LAYER ----
WITH
SRC_INV as ( SELECT *     from     STAGING.STG_INVOICE_HEADER ),
SRC_HDR_EDIT as ( SELECT *     from     STAGING.STG_INVOICE_HEADER_EDIT ),
SRC_EDIT as ( SELECT *     from     STAGING.STG_EDIT ),
SRC_HDR_EOB as ( SELECT *     from     STAGING.STG_INVOICE_HEADER_EOB ),
SRC_EOB as ( SELECT *     from     STAGING.STG_EOB ),
SRC_ENTRY_EDIT as ( SELECT *     from     STAGING.STG_EDIT_EOB ),
SRC_ENTRY_EOB as ( SELECT *     from     STAGING.STG_EDIT_EOB ),
//SRC_INV as ( SELECT *     from     STG_INVOICE_HEADER) ,
//SRC_HDR_EDIT as ( SELECT *     from     STG_INVOICE_HEADER_EDIT) ,
//SRC_EDIT as ( SELECT *     from     STG_EDIT) ,
//SRC_HDR_EOB as ( SELECT *     from     STG_INVOICE_HEADER_EOB) ,
//SRC_EOB as ( SELECT *     from     STG_EOB) ,
//SRC_ENTRY_EDIT as ( SELECT *     from     STG_EDIT_EOB) ,
//SRC_ENTRY_EOB as ( SELECT *     from     STG_EDIT_EOB) ,

---- LOGIC LAYER ----

LOGIC_INV as ( SELECT 
		  INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, TRIM( INVOICE_NUMBER )                             AS                                     INVOICE_NUMBER 
		, RECEIPT_DATE                                       AS                                       RECEIPT_DATE 
		from SRC_INV
            ),
LOGIC_HDR_EDIT as ( SELECT 
		  INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, TRIM( SOURCE )                                     AS                                             SOURCE 
		, TRIM( PHASE_APPLIED )                              AS                                      PHASE_APPLIED 
		, TRIM( DISPOSITION )                                AS                                        DISPOSITION 
		, EDIT_RID                                           AS                                           EDIT_RID 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		from SRC_HDR_EDIT
            ),
LOGIC_EDIT as ( SELECT 
		  EDIT_RID                                           AS                                           EDIT_RID 
		, TRIM( CODE )                                       AS                                               CODE 
		from SRC_EDIT
            ),
LOGIC_HDR_EOB as ( SELECT 
		  INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, TRIM( SOURCE )                                     AS                                             SOURCE 
		, TRIM( PHASE_APPLIED )                              AS                                      PHASE_APPLIED 
		,  NULL                           					 AS                                    EOB_DISPOSITION 
		, EOB_RID                                            AS                                            EOB_RID 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		from SRC_HDR_EOB
            ),
LOGIC_EOB as ( SELECT 
		  EOB_RID                                            AS                                            EOB_RID 
		, TRIM( CODE )                                       AS                                               CODE 
		from SRC_EOB
            ),
LOGIC_ENTRY_EDIT as ( SELECT 
		  EDIT_RID                                           AS                                           EDIT_RID 
		from SRC_ENTRY_EDIT
            ),
LOGIC_ENTRY_EOB as ( SELECT 
		  EOB_RID                                            AS                                            EOB_RID 
		from SRC_ENTRY_EOB
            )

---- RENAME LAYER ----
,

RENAME_INV as ( SELECT 
		  INVOICE_HEADER_ID                                  as                              INV_INVOICE_HEADER_ID
		, INVOICE_NUMBER                                     as                                     INVOICE_NUMBER
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE 
				FROM     LOGIC_INV   ), 
RENAME_HDR_EDIT as ( SELECT 
		  INVOICE_HEADER_ID                                  as                         HDR_EDIT_INVOICE_HEADER_ID
		, SOURCE                                             as                                    HDR_EDIT_SOURCE
		, PHASE_APPLIED                                      as                             HDR_EDIT_PHASE_APPLIED
		, DISPOSITION                                        as                               HDR_EDIT_DISPOSITION
		, EDIT_RID                                           as                                  HDR_EDIT_EDIT_RID
		, ENTRY_DATE                                         as                                    EDIT_ENTRY_DATE 
				FROM     LOGIC_HDR_EDIT   ), 
RENAME_EDIT as ( SELECT 
		  EDIT_RID                                           as                                           EDIT_RID
		, CODE                                               as                                          EDIT_CODE 
				FROM     LOGIC_EDIT   ), 
RENAME_HDR_EOB as ( SELECT 
		  INVOICE_HEADER_ID                                  as                          HDR_EOB_INVOICE_HEADER_ID
		, SOURCE                                             as                                     HDR_EOB_SOURCE
		, PHASE_APPLIED                                      as                              HDR_EOB_PHASE_APPLIED
		, EOB_DISPOSITION                                    as                                    EOB_DISPOSITION
		, EOB_RID                                            as                                    HDR_EOB_EOB_RID
		, ENTRY_DATE                                         as                                     EOB_ENTRY_DATE 
				FROM     LOGIC_HDR_EOB   ), 
RENAME_EOB as ( SELECT 
		  EOB_RID                                            as                                            EOB_RID
		, CODE                                               as                                           EOB_CODE 
				FROM     LOGIC_EOB   ), 
RENAME_ENTRY_EDIT as ( SELECT 
		  EDIT_RID                                           as                                     ENTRY_EDIT_RID 
				FROM     LOGIC_ENTRY_EDIT   ), 
RENAME_ENTRY_EOB as ( SELECT 
		  EOB_RID                                            as                                      ENTRY_EOB_RID 
				FROM     LOGIC_ENTRY_EOB   )

---- FILTER LAYER (uses aliases) ----
,

FILTER_INV                            as ( SELECT * from    RENAME_INV   ),
FILTER_HDR_EDIT                       as ( SELECT * from    RENAME_HDR_EDIT   ),
FILTER_EDIT                           as ( SELECT * from    RENAME_EDIT   ),
FILTER_HDR_EOB                        as ( SELECT * from    RENAME_HDR_EOB   ),
FILTER_EOB                            as ( SELECT * from    RENAME_EOB   ),
FILTER_ENTRY_EDIT                     as ( SELECT * from    RENAME_ENTRY_EDIT   ),
FILTER_ENTRY_EOB                      as ( SELECT * from    RENAME_ENTRY_EOB   ),

---- JOIN LAYER ----
HDR_EDIT_HDR_EOB as (SELECT HDR_EOB_INVOICE_HEADER_ID AS INVOICE_HEADER_ID
					        FROM FILTER_HDR_EOB
					 UNION
					 SELECT HDR_EDIT_INVOICE_HEADER_ID AS INVOICE_HEADER_ID
					        FROM FILTER_HDR_EDIT ),

INVOICE_HDR as ( SELECT * 
				FROM  HDR_EDIT_HDR_EOB
				LEFT JOIN FILTER_INV ON  FILTER_INV.INV_INVOICE_HEADER_ID =  HDR_EDIT_HDR_EOB.INVOICE_HEADER_ID  ),
HDR_EOB_EDIT as ( SELECT * 
				FROM  INVOICE_HDR
				LEFT JOIN FILTER_HDR_EOB ON  FILTER_HDR_EOB.HDR_EOB_INVOICE_HEADER_ID = INVOICE_HDR.INVOICE_HEADER_ID  
				LEFT JOIN FILTER_HDR_EDIT ON  FILTER_HDR_EDIT.HDR_EDIT_INVOICE_HEADER_ID =  INVOICE_HDR.INVOICE_HEADER_ID  ),
EDIT_EOB as ( SELECT * 
				FROM  HDR_EOB_EDIT
				LEFT JOIN FILTER_EDIT ON  FILTER_EDIT.EDIT_RID = HDR_EOB_EDIT.HDR_EDIT_EDIT_RID 
						LEFT JOIN FILTER_EOB ON  FILTER_EOB.EOB_RID = HDR_EOB_EDIT.HDR_EOB_EOB_RID ),
INV_HDR_EDIT_EOB as (SELECT * 
						FROM EDIT_EOB
						LEFT JOIN FILTER_ENTRY_EDIT ON FILTER_ENTRY_EDIT.ENTRY_EDIT_RID = EDIT_EOB.EDIT_RID
						LEFT JOIN FILTER_ENTRY_EOB ON FILTER_ENTRY_EOB.ENTRY_EOB_RID = EDIT_EOB.EOB_RID )

SELECT DISTINCT INV_INVOICE_HEADER_ID
				, INVOICE_NUMBER
				, RECEIPT_DATE
				, HDR_EDIT_INVOICE_HEADER_ID
				, HDR_EDIT_SOURCE
				, HDR_EDIT_PHASE_APPLIED
				, HDR_EDIT_DISPOSITION
				, HDR_EDIT_EDIT_RID
				, EDIT_ENTRY_DATE
				, EDIT_RID
				, EDIT_CODE
				, HDR_EOB_INVOICE_HEADER_ID
				, HDR_EOB_SOURCE
				, HDR_EOB_PHASE_APPLIED
				, EOB_DISPOSITION
				, HDR_EOB_EOB_RID
				, EOB_ENTRY_DATE
				, EOB_RID
				, EOB_CODE
				, ENTRY_EDIT_RID
				, ENTRY_EOB_RID
				, INVOICE_HEADER_ID
					FROM INV_HDR_EDIT_EOB