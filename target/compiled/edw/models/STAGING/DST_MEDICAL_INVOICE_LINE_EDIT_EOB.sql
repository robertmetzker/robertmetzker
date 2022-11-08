---- SRC LAYER ----
WITH
SRC_INV as ( SELECT *     from     STAGING.DST_INVOICE ),
SRC_LINE_EDIT as ( SELECT *     from     STAGING.STG_INVOICE_LINE_EDIT ),
SRC_EDIT as ( SELECT *     from     STAGING.STG_EDIT ),
SRC_LINE_EOB as ( SELECT *     from     STAGING.STG_INVOICE_LINE_EOB ),
SRC_EOB as ( SELECT *     from     STAGING.STG_EOB ),
SRC_ENTRY_EDIT as ( SELECT *     from     STAGING.STG_EDIT_EOB ),
SRC_ENTRY_EOB as ( SELECT *     from     STAGING.STG_EDIT_EOB ),
SRC_LINE_EDIT_ID as ( SELECT *     from     STAGING.STG_INVOICE_LINE_EDIT ),
SRC_LINE_EOB_ID as ( SELECT *     from     STAGING.STG_INVOICE_LINE_EOB ),

//SRC_INV as ( SELECT *     from     DST_INVOICE) ,
//SRC_LINE_EDIT as ( SELECT *     from     STG_INVOICE_LINE_EDIT) ,
//SRC_EDIT as ( SELECT *     from     STG_EDIT) ,
//SRC_LINE_EOB as ( SELECT *     from     STG_INVOICE_LINE_EOB) ,
//SRC_EOB as ( SELECT *     from     STG_EOB) ,
//SRC_ENTRY_EDIT as ( SELECT *     from     STG_EDIT_EOB) ,
//SRC_ENTRY_EOB as ( SELECT *     from     STG_EDIT_EOB) ,
//SRC_LINE_EDIT_ID as ( SELECT *     from     STG_INVOICE_LINE_EDIT) ,
//SRC_LINE_EOB_ID as ( SELECT *     from     STG_INVOICE_LINE_EOB) ,

---- LOGIC LAYER ----

LOGIC_INV as ( SELECT 
		  INVOICE_LINE_ID                                    AS                                    INVOICE_LINE_ID 
		, TRIM( INVOICE_NUMBER )                             AS                                     INVOICE_NUMBER 
		, LINE_SEQUENCE                                      AS                                      LINE_SEQUENCE 
		, RECEIPT_DATE                                       AS                                       RECEIPT_DATE 
		from SRC_INV
            ),
LOGIC_LINE_EDIT as ( SELECT 
		  INVOICE_LINE_ID                                    AS                                    INVOICE_LINE_ID 
		, TRIM( SOURCE )                                     AS                                             SOURCE 
		, TRIM( PHASE_APPLIED )                              AS                                      PHASE_APPLIED 
		, TRIM( DISPOSITION )                                AS                                        DISPOSITION 
		, EDIT_RID                                           AS                                           EDIT_RID 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		from SRC_LINE_EDIT
            ),
LOGIC_EDIT as ( SELECT 
		  EDIT_RID                                           AS                                           EDIT_RID 
		, TRIM( CODE )                                       AS                                               CODE 
		from SRC_EDIT
            ),
LOGIC_LINE_EOB as ( SELECT 
		  INVOICE_LINE_ID                                    AS                                    INVOICE_LINE_ID 
		, TRIM( SOURCE )                                     AS                                             SOURCE 
		, TRIM( PHASE_APPLIED )                              AS                                      PHASE_APPLIED 
		  --- NEW EOB_DISPOSITION                            AS                                          (DERIVED) 
		, EOB_RID                                            AS                                            EOB_RID 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		from SRC_LINE_EOB
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
            ),
LOGIC_LINE_EDIT_ID as ( SELECT 
		  INVOICE_LINE_ID                                            AS                                            INVOICE_LINE_ID 
		from SRC_LINE_EDIT_ID
            ),
LOGIC_LINE_EOB_ID as ( SELECT 
		  INVOICE_LINE_ID                                            AS                                            INVOICE_LINE_ID 
		from SRC_LINE_EOB_ID
            )            




---- RENAME LAYER ----
,

RENAME_INV as ( SELECT 
		  INVOICE_LINE_ID                                    as                                    INV_INVOICE_LINE_ID
		, INVOICE_NUMBER                                     as                                     INVOICE_NUMBER
		, LINE_SEQUENCE                                      as                                      LINE_SEQUENCE
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE 
				FROM     LOGIC_INV   ), 
RENAME_LINE_EDIT as ( SELECT 
		  INVOICE_LINE_ID                                    as                          LINE_EDIT_INVOICE_LINE_ID
		, SOURCE                                             as                                   LINE_EDIT_SOURCE
		, PHASE_APPLIED                                      as                            LINE_EDIT_PHASE_APPLIED
		, DISPOSITION                                        as                              LINE_EDIT_DISPOSITION
		, EDIT_RID                                           as                                 LINE_EDIT_EDIT_RID
		, ENTRY_DATE                                         as                                    EDIT_ENTRY_DATE 
				FROM     LOGIC_LINE_EDIT   ), 
RENAME_EDIT as ( SELECT 
		  EDIT_RID                                           as                                           EDIT_RID
		, CODE                                               as                                          EDIT_CODE 
				FROM     LOGIC_EDIT   ), 
RENAME_LINE_EOB as ( SELECT 
		  INVOICE_LINE_ID                                    as                           LINE_EOB_INVOICE_LINE_ID
		, SOURCE                                             as                                    LINE_EOB_SOURCE
		, PHASE_APPLIED                                      as                             LINE_EOB_PHASE_APPLIED
		, EOB_RID                                            as                                   LINE_EOB_EOB_RID
		, ENTRY_DATE                                         as                                     EOB_ENTRY_DATE 
				FROM     LOGIC_LINE_EOB   ), 
RENAME_EOB as ( SELECT 
		  EOB_RID                                            as                                            EOB_RID
		, CODE                                               as                                           EOB_CODE 
				FROM     LOGIC_EOB   ), 
RENAME_ENTRY_EDIT as ( SELECT 
		  EDIT_RID                                           as                                     ENTRY_EDIT_RID 
				FROM     LOGIC_ENTRY_EDIT   ), 
RENAME_ENTRY_EOB as ( SELECT 
		  EOB_RID                                            as                                      ENTRY_EOB_RID 
				FROM     LOGIC_ENTRY_EOB   ),
RENAME_LINE_EDIT_ID as ( SELECT 
		  INVOICE_LINE_ID                                   as                                      INVOICE_LINE_ID
 
				FROM     LOGIC_LINE_EDIT_ID   ),
RENAME_LINE_EOB_ID as ( SELECT 
		  INVOICE_LINE_ID                                            as                                      INVOICE_LINE_ID 
				FROM     LOGIC_LINE_EOB_ID   )                

---- FILTER LAYER (uses aliases) ----
,
FILTER_INV                            as ( SELECT * from    RENAME_INV   ),
FILTER_LINE_EDIT                      as ( SELECT * from    RENAME_LINE_EDIT   ),
FILTER_EDIT                           as ( SELECT * from    RENAME_EDIT   ),
FILTER_LINE_EOB                       as ( SELECT * from    RENAME_LINE_EOB   ),
FILTER_EOB                            as ( SELECT * from    RENAME_EOB   ),
FILTER_ENTRY_EDIT                     as ( SELECT * from    RENAME_ENTRY_EDIT   ),
FILTER_ENTRY_EOB                      as ( SELECT * from    RENAME_ENTRY_EOB   ),
FILTER_LINE_EDIT_ID                     as ( SELECT * from    RENAME_LINE_EDIT_ID   ),
FILTER_LINE_EOB_ID                      as ( SELECT * from    RENAME_LINE_EOB_ID   ),
---- JOIN LAYER ----


JOIN_UNION AS (

        SELECT  INVOICE_LINE_ID FROM FILTER_LINE_EOB_ID
        UNION 
        SELECT  INVOICE_LINE_ID FROM FILTER_LINE_EDIT_ID

),


JOIN_UNION_LEFT AS  (

        select * from JOIN_UNION JU
        LEFT JOIN FILTER_INV INV ON JU.INVOICE_LINE_ID = INV.INV_INVOICE_LINE_ID
        LEFT JOIN FILTER_LINE_EDIT LINE_EDIT ON JU.INVOICE_LINE_ID = LINE_EDIT.LINE_EDIT_INVOICE_LINE_ID
        LEFT JOIN FILTER_EDIT EDIT ON LINE_EDIT.LINE_EDIT_EDIT_RID = EDIT.EDIT_RID
        LEFT JOIN FILTER_LINE_EOB LINE_EOB ON JU.INVOICE_LINE_ID = LINE_EOB.LINE_EOB_INVOICE_LINE_ID
        LEFT JOIN FILTER_EOB EOB ON LINE_EOB.LINE_EOB_EOB_RID = EOB.EOB_RID
        LEFT JOIN FILTER_ENTRY_EOB ENTRY_EOB ON EOB.EOB_RID = ENTRY_EOB.ENTRY_EOB_RID
        LEFT JOIN FILTER_ENTRY_EDIT ENTRY_EDIT ON EDIT.EDIT_RID = ENTRY_EDIT.ENTRY_EDIT_RID

),

-- ETL Layer --
          ETL AS (      select distinct 
            
                  
                INVOICE_NUMBER	,
                LINE_SEQUENCE	,
                RECEIPT_DATE	,
                LINE_EDIT_SOURCE	,
                LINE_EDIT_PHASE_APPLIED	,
                LINE_EDIT_DISPOSITION	,
                LINE_EDIT_EDIT_RID	,
                EDIT_ENTRY_DATE	,
                EDIT_RID	,
                EDIT_CODE	,
                LINE_EOB_SOURCE	,
                LINE_EOB_PHASE_APPLIED	,
                NULL AS EOB_DISPOSITION	,
                LINE_EOB_EOB_RID	,
                EOB_ENTRY_DATE	,
                EOB_RID	,
                EOB_CODE	,
                ENTRY_EDIT_RID	,
                ENTRY_EOB_RID	
              
                  
                  from JOIN_UNION_LEFT
				  
				   ),

	--
	ETL_KEY AS		(
   select md5(cast(
    
    coalesce(cast(INVOICE_NUMBER as 
    varchar
), '') || '-' || coalesce(cast(LINE_SEQUENCE as 
    varchar
), '') || '-' || coalesce(cast(EDIT_CODE as 
    varchar
), '') || '-' || coalesce(cast(EOB_CODE as 
    varchar
), '') || '-' || coalesce(cast(LINE_EDIT_SOURCE as 
    varchar
), '') || '-' || coalesce(cast(LINE_EOB_SOURCE as 
    varchar
), '') || '-' || coalesce(cast(LINE_EDIT_PHASE_APPLIED as 
    varchar
), '') || '-' || coalesce(cast(LINE_EOB_PHASE_APPLIED as 
    varchar
), '') || '-' || coalesce(cast(EOB_ENTRY_DATE as 
    varchar
), '') || '-' || coalesce(cast(EDIT_ENTRY_DATE as 
    varchar
), '')

 as 
    varchar
)) as INV_INVOICE_LINE_ID
   , * from  ETL

	)	   
                  
                  select *  from ETL_KEY