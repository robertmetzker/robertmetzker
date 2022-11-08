---- SRC LAYER ----
WITH
SRC_EDIT as ( SELECT *     from     DEV_VIEWS.BASE.INVOICE_LINE_EDIT ),
SRC_LINE as ( SELECT *     from     DEV_VIEWS.BASE.INVOICE_LINE ),
SRC_HDR as ( SELECT *     from     DEV_VIEWS.BASE.INVOICE_HEADER ),
//SRC_EDIT as ( SELECT *     from     INVOICE_LINE_EDIT) ,
//SRC_LINE as ( SELECT *     from     INVOICE_LINE) ,
//SRC_HDR as ( SELECT *     from     INVOICE_HEADER) ,

---- LOGIC LAYER ----

LOGIC_EDIT as ( SELECT 
		  INVOICE_LINE_ID                                    AS                                    INVOICE_LINE_ID 
		, EDIT_RID                                           AS                                           EDIT_RID 
		, upper( TRIM( SOURCE ) )                            AS                                             SOURCE 
		, cast( ENTRY_DATE as DATE )                         AS                                         ENTRY_DATE 
		, PHASE_APPLIED                                      AS                                      PHASE_APPLIED 
		, upper( TRIM( DISPOSITION ) )                       AS                                        DISPOSITION 
		from SRC_EDIT
            ),
LOGIC_LINE as ( SELECT 
		  INVOICE_LINE_ID                                    AS                                    INVOICE_LINE_ID 
		, INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, LINE_SEQUENCE                                      AS                                      LINE_SEQUENCE 
		, SEQUENCE_EXTENSION                                 AS                                 SEQUENCE_EXTENSION 
		, VERSION_NUMBER                                     AS                                     VERSION_NUMBER 
		from SRC_LINE
            ),
LOGIC_HDR as ( SELECT 
		  TO_CHAR(RECEIPT_DATE, 'MM/DD/YYYY')||' '|| LPAD(BATCH_NUMBER, 4, '0') ||' '|| LPAD(BATCH_SEQUENCE, 2, '0') ||' '|| LPAD(EXTENSION_NUMBER, 2, '0') AS INVOICE_NUMBER
		, INVOICE_HEADER_ID                                  AS                                  INVOICE_HEADER_ID 
		, BATCH_SEQUENCE                                     AS                                     BATCH_SEQUENCE 
		, EXTENSION_NUMBER                                   AS                                   EXTENSION_NUMBER 
		, VERSION_NUMBER                                     AS                                     VERSION_NUMBER 
		, BATCH_NUMBER                                       AS                                       BATCH_NUMBER 
		, cast( RECEIPT_DATE as DATE )                       AS                                       RECEIPT_DATE 
		from SRC_HDR
            )

---- RENAME LAYER ----
,

RENAME_EDIT as ( SELECT 
		  INVOICE_LINE_ID                                    as                                    INVOICE_LINE_ID
		, EDIT_RID                                           as                                           EDIT_RID
		, SOURCE                                             as                                             SOURCE
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, PHASE_APPLIED                                      as                                      PHASE_APPLIED
		, DISPOSITION                                        as                                        DISPOSITION 
				FROM     LOGIC_EDIT   ), 
RENAME_LINE as ( SELECT 
		  INVOICE_LINE_ID                                    as                               LINE_INVOICE_LINE_ID
		, INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID
		, LINE_SEQUENCE                                      as                                      LINE_SEQUENCE
		, SEQUENCE_EXTENSION                                 as                                 SEQUENCE_EXTENSION
		, VERSION_NUMBER                                     as                                LINE_VERSION_NUMBER 
				FROM     LOGIC_LINE   ), 
RENAME_HDR as ( SELECT 
		  INVOICE_HEADER_ID                                  as                              HDR_INVOICE_HEADER_ID
		, INVOICE_NUMBER                                     as                                     INVOICE_NUMBER
		, BATCH_SEQUENCE                                     as                                     BATCH_SEQUENCE
		, EXTENSION_NUMBER                                   as                                   EXTENSION_NUMBER
		, VERSION_NUMBER                                     as                                     VERSION_NUMBER
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE 
				FROM     LOGIC_HDR   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EDIT                           as ( SELECT * from    RENAME_EDIT   ),
FILTER_LINE                           as ( SELECT * from    RENAME_LINE   ),
FILTER_HDR                            as ( SELECT * from    RENAME_HDR   ),

---- JOIN LAYER ----

EDIT as ( SELECT * 
				FROM  FILTER_EDIT
				INNER JOIN FILTER_LINE ON  FILTER_EDIT.INVOICE_LINE_ID =  FILTER_LINE.LINE_INVOICE_LINE_ID 
				INNER JOIN FILTER_HDR ON  FILTER_LINE.INVOICE_HEADER_ID =  FILTER_HDR.HDR_INVOICE_HEADER_ID  )
SELECT * 
from EDIT