---- SRC LAYER ----
WITH
SRC_EDIT as ( SELECT *     from     DEV_VIEWS.BASE.INVOICE_HEADER_EDIT ),
SRC_HDR as ( SELECT *     from     DEV_VIEWS.BASE.INVOICE_HEADER ),
//SRC_EDIT as ( SELECT *     from     INVOICE_HEADER_EDIT) ,
//SRC_HDR as ( SELECT *     from     INVOICE_HEADER) ,

---- LOGIC LAYER ----

LOGIC_EDIT as ( SELECT 
		  INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, EDIT_RID                                           AS                                           EDIT_RID 
		, upper( TRIM( SOURCE ) )                            AS                                             SOURCE 
		, cast( ENTRY_DATE as DATE )                         AS                                         ENTRY_DATE 
		, PHASE_APPLIED                                      AS                                      PHASE_APPLIED 
		, upper( TRIM( DISPOSITION ) )                       AS                                        DISPOSITION 
		from SRC_EDIT
            ),
LOGIC_HDR as ( SELECT 
		  TO_CHAR(RECEIPT_DATE, 'MM/DD/YYYY')||' '|| LPAD(BATCH_NUMBER, 4, '0') ||' '|| LPAD(BATCH_SEQUENCE, 2, '0') ||' '|| LPAD(EXTENSION_NUMBER, 2, '0') AS INVOICE_NUMBER
		, INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, cast( RECEIPT_DATE as DATE )                       AS                                       RECEIPT_DATE 
		, BATCH_NUMBER                                       AS                                       BATCH_NUMBER 
		, BATCH_SEQUENCE                                     AS                                     BATCH_SEQUENCE 
		from SRC_HDR
            )

---- RENAME LAYER ----
,

RENAME_EDIT as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID
		, EDIT_RID                                           as                                           EDIT_RID
		, SOURCE                                             as                                             SOURCE
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, PHASE_APPLIED                                      as                                      PHASE_APPLIED
		, DISPOSITION                                        as                                        DISPOSITION 
				FROM     LOGIC_EDIT   ), 
RENAME_HDR as ( SELECT 
		  INVOICE_HEADER_ID                                  as                              HDR_INVOICE_HEADER_ID
		, INVOICE_NUMBER                                     as                                     INVOICE_NUMBER
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER
		, BATCH_SEQUENCE                                     as                                     BATCH_SEQUENCE 
				FROM     LOGIC_HDR   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EDIT                           as ( SELECT * from    RENAME_EDIT   ),
FILTER_HDR                            as ( SELECT * from    RENAME_HDR   ),

---- JOIN LAYER ----

EDIT as ( SELECT * 
				FROM  FILTER_EDIT
				INNER JOIN FILTER_HDR ON  FILTER_EDIT.INVOICE_HEADER_ID =  FILTER_HDR.HDR_INVOICE_HEADER_ID  )
SELECT * 
from EDIT